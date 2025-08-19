"""Fetch Wildberries goods prices and store them in CSV/SQLite/ODBC."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger, setup_logging
from finmodel.utils.db_load import load_wb_tokens
from finmodel.utils.paths import get_db_path

WB_ENDPOINT = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
TIMEOUT = 15
SLEEP_BETWEEN_BATCHES_SEC = 0.4
PAGE_LIMIT = 1000

logger = get_logger(__name__)


# ───────────────────────────── helpers ───────────────────────────── #


def iter_chunks(lst: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def make_http(api_key: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.headers.update({"Accept": "application/json", "User-Agent": "WB-SPP-Fetcher/1.0 (+PowerBI)"})
    if api_key:
        s.headers["Authorization"] = api_key
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def fetch_batch(
    http: requests.Session,
    nm_id: Optional[str] = None,
    limit: int = PAGE_LIMIT,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Fetch a batch of goods from WB API.

    Returns list of dicts: nmId, sizeID, price, discountedPrice, discount.
    """
    params: Dict[str, Any] = {}
    if nm_id is not None:
        params["filterNmID"] = nm_id
    else:
        params.update({"limit": limit, "offset": offset})

    r = http.get(WB_ENDPOINT, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    payload: Dict[str, Any] = r.json() or {}
    data: Any = payload.get("data", {})
    products: List[Dict[str, Any]]
    if isinstance(data, dict):
        products = data.get("products") or data.get("listGoods") or data.get("goods") or []
    else:
        products = data or []

    out: List[Dict[str, Any]] = []
    for p in products:
        nm_raw: Any = p.get("nmID") or p.get("nmId") or p.get("id")
        nm_str: Optional[str] = str(nm_raw) if nm_raw is not None else None
        sizes_any: Any = p.get("sizes") or []
        sizes: List[Dict[str, Any]] = sizes_any if isinstance(sizes_any, list) else []

        if not sizes:
            out.append(
                {
                    "nmId": nm_str,
                    "sizeID": None,
                    "price": p.get("price") if isinstance(p.get("price"), (int, float)) else None,
                    "discountedPrice": (
                        p.get("discountedPrice")
                        if isinstance(p.get("discountedPrice"), (int, float))
                        else None
                    ),
                    "discount": (
                        float(p.get("discount"))
                        if isinstance(p.get("discount"), (int, float))
                        else None
                    ),
                }
            )
            continue

        for s in sizes:
            price: Optional[float] = (
                s.get("price")
                if isinstance(s.get("price"), (int, float))
                else (p.get("price") if isinstance(p.get("price"), (int, float)) else None)
            )
            discounted_price: Optional[float] = (
                s.get("discountedPrice")
                if isinstance(s.get("discountedPrice"), (int, float))
                else (
                    p.get("discountedPrice")
                    if isinstance(p.get("discountedPrice"), (int, float))
                    else None
                )
            )
            disc_val: Any = s.get("discount")
            if disc_val is None:
                disc_val = p.get("discount")
            discount: Optional[float] = (
                float(disc_val) if isinstance(disc_val, (int, float)) else None
            )

            size_raw: Any = s.get("sizeID") or s.get("sizeId") or s.get("id")
            size_id: Optional[str] = str(size_raw) if size_raw is not None else None

            out.append(
                {
                    "nmId": nm_str,
                    "sizeID": size_id,
                    "price": price,
                    "discountedPrice": discounted_price,
                    "discount": discount,
                }
            )
    return out


def calc_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    price: Optional[float] = row.get("price")
    discounted: Optional[float] = row.get("discountedPrice")
    discount: Optional[float] = row.get("discount")

    discount_total_pct: Optional[float] = (
        (1 - (discounted / price)) * 100.0
        if price is not None and discounted is not None and price != 0
        else None
    )
    spp_pct_approx: Optional[float] = (
        discount_total_pct - discount
        if discount_total_pct is not None and discount is not None
        else None
    )

    return {
        **row,
        "price_rub": float(price) if isinstance(price, (int, float)) else None,
        "salePrice_rub": float(discounted) if isinstance(discounted, (int, float)) else None,
        "discount_total_pct": discount_total_pct,
        "spp_pct_approx": spp_pct_approx,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ───────────────────────────── IO: sources ───────────────────────────── #


def read_nmids_from_csv(path: str, col: str) -> List[str]:
    nmids: List[str] = []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if col not in (reader.fieldnames or []):
            raise SystemExit(f"Колонка '{col}' не найдена в CSV. Нашёл: {reader.fieldnames}")
        for row in reader:
            v = (row.get(col) or "").strip()
            if v:
                nmids.append(v)
    return nmids


def read_nmids_from_txt(path: str) -> List[str]:
    nmids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            v = line.strip()
            if v:
                nmids.append(v)
    return nmids


def read_nmids_from_sqlite(db_path: str, sql: Optional[str]) -> List[str]:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"SQLite файл не найден: {p}")

    rows: List[sqlite3.Row] = []
    with sqlite3.connect(str(p)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            if not sql:
                try_sql = [
                    "SELECT DISTINCT nmId AS nmId FROM katalog WHERE nmId IS NOT NULL",
                    "SELECT DISTINCT nm_id AS nmId FROM katalog WHERE nm_id IS NOT NULL",
                ]
                for q in try_sql:
                    try:
                        rows = cur.execute(q).fetchall()
                        if rows:
                            sql = q
                            break
                    except sqlite3.Error:
                        continue
                if not rows:
                    raise SystemExit(
                        "Не удалось автоматически найти колонку nmId / nm_id в таблице 'katalog'. "
                        "Укажи SQL вручную через --sql."
                    )
            else:
                rows = cur.execute(sql).fetchall()
        finally:
            cur.close()

    nmids = [
        str(r["nmId"]).strip()
        for r in rows
        if r["nmId"] is not None and str(r["nmId"]).strip() != ""
    ]
    return nmids


def read_nmids_for_org(conn: sqlite3.Connection, org_id: int) -> List[str]:
    """Return ``nmId`` values for a given organization."""

    try_sql = [
        "SELECT DISTINCT nmID AS nmId FROM katalog WHERE org_id = ? AND nmID IS NOT NULL",
        "SELECT DISTINCT nm_id AS nmId FROM katalog WHERE org_id = ? AND nm_id IS NOT NULL",
    ]

    for q in try_sql:
        try:
            rows = conn.execute(q, (org_id,)).fetchall()
            if rows:
                return [
                    str(r["nmId"]).strip()
                    for r in rows
                    if r["nmId"] is not None and str(r["nmId"]).strip() != ""
                ]
        except sqlite3.Error:
            continue
    return []


# ───────────────────────────── IO: sinks ───────────────────────────── #

CSV_FIELDS: List[str] = [
    "nmId",
    "sizeID",
    "price",
    "discountedPrice",
    "discount",
    "price_rub",
    "salePrice_rub",
    "discount_total_pct",
    "spp_pct_approx",
    "updated_at_utc",
]


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in CSV_FIELDS})


def write_to_sqlite(db_path: str, rows: List[Dict[str, Any]], table: str = "spp") -> int:
    if not rows:
        logger.warning("Нет строк для записи в SQLite — пропускаю.")
        return 0

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                nmId TEXT,
                sizeID TEXT,
                price REAL,
                discountedPrice REAL,
                discount REAL,
                price_rub REAL,
                salePrice_rub REAL,
                discount_total_pct REAL,
                spp_pct_approx REAL,
                updated_at_utc TEXT,
                PRIMARY KEY (nmId, sizeID)
            )
            """
        )
        data: List[Tuple[Any, ...]] = [
            (
                r.get("nmId"),
                r.get("sizeID"),
                r.get("price"),
                r.get("discountedPrice"),
                r.get("discount"),
                r.get("price_rub"),
                r.get("salePrice_rub"),
                r.get("discount_total_pct"),
                r.get("spp_pct_approx"),
                r.get("updated_at_utc"),
            )
            for r in rows
        ]
        cur.executemany(
            f"""
            INSERT OR REPLACE INTO {table}
            (nmId, sizeID, price, discountedPrice, discount, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        conn.commit()
    logger.info("Записано строк в SQLite: %s", len(rows))
    return len(rows)


def write_prices_to_db(db_path: str, rows: List[Dict[str, Any]]) -> int:
    """Persist rows into ``WBGoodsPricesFlat`` table inside ``finmodel.db``."""

    if not rows:
        logger.warning("Нет строк для записи в БД — пропускаю.")
        return 0

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS WBGoodsPricesFlat (
                org_id INTEGER,
                nmId TEXT,
                sizeID TEXT,
                price REAL,
                discountedPrice REAL,
                discount REAL,
                price_rub REAL,
                salePrice_rub REAL,
                discount_total_pct REAL,
                spp_pct_approx REAL,
                updated_at_utc TEXT,
                PRIMARY KEY (org_id, nmId, sizeID)
            )
            """
        )
        cur.execute("DELETE FROM WBGoodsPricesFlat")
        data = [
            (
                r.get("org_id"),
                r.get("nmId"),
                r.get("sizeID"),
                r.get("price"),
                r.get("discountedPrice"),
                r.get("discount"),
                r.get("price_rub"),
                r.get("salePrice_rub"),
                r.get("discount_total_pct"),
                r.get("spp_pct_approx"),
                r.get("updated_at_utc"),
            )
            for r in rows
        ]
        cur.executemany(
            """
            INSERT OR REPLACE INTO WBGoodsPricesFlat
            (org_id, nmId, sizeID, price, discountedPrice, discount,
             price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        conn.commit()
    logger.info("Записано строк в базу: %s", len(rows))
    return len(rows)


def write_to_db_odbc(
    rows: List[Dict[str, Any]], dsn: str, table: str = "dbo.WBGoodsPricesFlat"
) -> int:
    """Вставка через ODBC (например, в MS SQL Server).
    Таблица должна существовать с колонками, соответствующими CSV_FIELDS.
    """
    import pyodbc  # type: ignore

    if not rows:
        logger.warning("Нет строк для записи в ODBC — пропускаю.")
        return 0

    cn = pyodbc.connect(dsn, autocommit=False)
    try:
        cur = cn.cursor()
        cur.execute(f"TRUNCATE TABLE {table}")
        data: List[Tuple[Any, ...]] = [
            (
                r.get("nmId"),
                r.get("sizeID"),
                r.get("price"),
                r.get("discountedPrice"),
                r.get("discount"),
                r.get("price_rub"),
                r.get("salePrice_rub"),
                r.get("discount_total_pct"),
                r.get("spp_pct_approx"),
                r.get("updated_at_utc"),
            )
            for r in rows
        ]
        cur.fast_executemany = True
        cur.executemany(
            f"""
            INSERT INTO {table}
            (nmId, sizeID, price, discountedPrice, discount, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        cn.commit()
    finally:
        cn.close()

    logger.info("Записано строк в ODBC: %s", len(rows))
    return len(rows)


# ───────────────────────────── pipeline ───────────────────────────── #


def import_prices(
    nmids: Optional[List[str]],
    dsn: Optional[str],
    api_key: Optional[str] = None,
    http: Optional[requests.Session] = None,
) -> int:
    if not api_key:
        raise ValueError("WB API key is required")
    http = http or make_http(api_key)
    all_rows: List[Dict[str, Any]] = []
    if nmids:
        for nm in nmids:
            raw_rows = fetch_batch(http, nm_id=nm)
            all_rows.extend(calc_metrics(r) for r in raw_rows)
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)
    else:
        offset = 0
        while True:
            raw_rows = fetch_batch(http, limit=PAGE_LIMIT, offset=offset)
            if not raw_rows:
                break
            all_rows.extend(calc_metrics(r) for r in raw_rows)
            offset += PAGE_LIMIT
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

    written = 0
    if dsn:
        written = write_to_db_odbc(all_rows, dsn)
    return written


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    src_group = parser.add_mutually_exclusive_group(required=False)
    src_group.add_argument("--csv", help="CSV-файл с колонкой nmId")
    src_group.add_argument("--txt", help="TXT-файл со списком nmId")
    src_group.add_argument("--sqlite", help="SQLite-файл с nmId")
    parser.add_argument("--col", default="nmId", help="Имя колонки для CSV")
    parser.add_argument("--sql", help="SQL-запрос для извлечения nmId из SQLite")
    parser.add_argument("--api-key", help="API key for Authorization header")

    parser.add_argument("--out-csv", help="Путь к выходному CSV")
    parser.add_argument("--out-sqlite", help="SQLite для записи результатов")
    parser.add_argument("--out-odbc", help="ODBC DSN для записи результатов")
    parser.add_argument(
        "--odbc-table", default="dbo.WBGoodsPricesFlat", help="Таблица для записи через ODBC"
    )

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    setup_logging()
    args = parse_args(argv)

    try:
        nmids_override: Optional[List[str]]
        if args.csv:
            nmids_override = read_nmids_from_csv(args.csv, args.col)
        elif args.txt:
            nmids_override = read_nmids_from_txt(args.txt)
        elif args.sqlite:
            nmids_override = read_nmids_from_sqlite(args.sqlite, args.sql)
        else:
            nmids_override = None

        db_path = str(get_db_path())
        if args.api_key:
            tokens: List[Tuple[Optional[int], str]] = [(None, args.api_key)]
        else:
            tokens = load_wb_tokens(db_path)
            if not tokens:
                logger.error("Не найдены токены в базе данных")
                raise SystemExit(1)

        rows_out: List[Dict[str, Any]] = []
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            for org_id, token in tokens:
                http = make_http(token)
                if nmids_override is not None:
                    nmids = nmids_override
                elif org_id is not None:
                    nmids = read_nmids_for_org(conn, org_id)
                    logger.info("Орг %s: найдено nmId: %s", org_id, len(nmids))
                else:
                    nmids = []

                if nmids:
                    for nm in nmids:
                        try:
                            batch = fetch_batch(http, nm_id=nm)
                        except Exception:
                            logger.exception("Ошибка при запросе nmID: %s", nm)
                            raise SystemExit(1)
                        for row in batch:
                            enriched = calc_metrics(row)
                            enriched["org_id"] = org_id
                            rows_out.append(enriched)
                        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)
                else:
                    offset = 0
                    while True:
                        try:
                            batch = fetch_batch(http, limit=PAGE_LIMIT, offset=offset)
                        except Exception:
                            logger.exception("Ошибка при запросе offset: %s", offset)
                            raise SystemExit(1)
                        if not batch:
                            break
                        for row in batch:
                            enriched = calc_metrics(row)
                            enriched["org_id"] = org_id
                            rows_out.append(enriched)
                        offset += PAGE_LIMIT
                        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        write_prices_to_db(db_path, rows_out)
        if args.out_csv:
            write_csv(args.out_csv, rows_out)
        if args.out_sqlite:
            write_to_sqlite(args.out_sqlite, rows_out, table="WBGoodsPricesFlat")
        if args.out_odbc:
            write_to_db_odbc(rows_out, args.out_odbc, table=args.odbc_table)

        logger.info("Обработано строк: %s", len(rows_out))
    except SystemExit:
        raise
    except Exception:
        logger.exception("Необработанная ошибка")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
