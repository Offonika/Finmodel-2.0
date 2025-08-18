"""Fetch Wildberries goods prices and store them in a database."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger, setup_logging

WB_ENDPOINT = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
CHUNK_SIZE = 100
TIMEOUT = 15
SLEEP_BETWEEN_BATCHES_SEC = 0.4

logger = get_logger(__name__)


def iter_chunks(lst: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def make_http(api_key: str | None = None) -> requests.Session:
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
    http: requests.Session, limit: int, offset: int, filter_nm_id: str | None = None
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if filter_nm_id:
        params["filterNmID"] = filter_nm_id
    r = http.get(WB_ENDPOINT, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    data = (payload or {}).get("data", {})
    if isinstance(data, dict):
        products = data.get("products") or data.get("listGoods") or data.get("goods") or []
    else:
        products = data or []

    out: List[Dict[str, Any]] = []
    for p in products:
        nm_id = (
            str(p.get("nmId") or p.get("nmID") or p.get("id"))
            if (p.get("nmId") or p.get("nmID") or p.get("id")) is not None
            else None
        )
        sizes = p.get("sizes") or []
        if not isinstance(sizes, list):
            sizes = []
        for s in sizes:
            out.append(
                {
                    "nmId": nm_id,
                    "sizeID": (
                        str(s.get("sizeId") or s.get("sizeID") or s.get("id"))
                        if (s.get("sizeId") or s.get("sizeID") or s.get("id")) is not None
                        else None
                    ),
                    "price": s.get("price") if isinstance(s.get("price"), int) else None,
                    "discountedPrice": (
                        s.get("discountedPrice")
                        if isinstance(s.get("discountedPrice"), int)
                        else None
                    ),
                    "clubDiscountedPrice": (
                        s.get("clubDiscountedPrice")
                        if isinstance(s.get("clubDiscountedPrice"), int)
                        else None
                    ),
                    "discount": (
                        float(s.get("discount"))
                        if isinstance(s.get("discount"), (int, float))
                        else None
                    ),
                    "clubDiscount": (
                        float(s.get("clubDiscount"))
                        if isinstance(s.get("clubDiscount"), (int, float))
                        else None
                    ),
                }
            )
    return out


def calc_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    price = row.get("price")
    discounted_price = row.get("discountedPrice")
    club_discounted_price = row.get("clubDiscountedPrice")
    discount = row.get("discount")
    club_discount = row.get("clubDiscount")

    discount_total_pct = (
        (1 - (discounted_price / price)) * 100.0
        if price is not None and discounted_price is not None and price != 0
        else None
    )
    club_discount_total_pct = (
        (1 - (club_discounted_price / price)) * 100.0
        if price is not None and club_discounted_price is not None and price != 0
        else None
    )
    spp_pct_approx = (
        discount_total_pct - discount - club_discount
        if discount_total_pct is not None and discount is not None and club_discount is not None
        else None
    )

    return {
        **row,
        "price_rub": (price / 100.0) if isinstance(price, int) else None,
        "discountedPrice_rub": (
            (discounted_price / 100.0) if isinstance(discounted_price, int) else None
        ),
        "clubDiscountedPrice_rub": (
            (club_discounted_price / 100.0) if isinstance(club_discounted_price, int) else None
        ),
        "discount_total_pct": discount_total_pct,
        "club_discount_total_pct": club_discount_total_pct,
        "spp_pct_approx": spp_pct_approx,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


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


def read_nmids_from_sqlite(db_path: str, sql: str | None) -> List[str]:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"SQLite файл не найден: {p}")

    rows: list[sqlite3.Row] = []
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


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "nmId",
        "sizeID",
        "price",
        "discountedPrice",
        "clubDiscountedPrice",
        "discount",
        "clubDiscount",
        "price_rub",
        "discountedPrice_rub",
        "clubDiscountedPrice_rub",
        "discount_total_pct",
        "club_discount_total_pct",
        "spp_pct_approx",
        "updated_at_utc",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})


def write_to_db_odbc(rows: List[Dict[str, Any]], dsn: str, table: str = "dbo.spp") -> int:
    import pyodbc

    if not rows:
        logger.warning("Нет строк для записи в БД — пропускаю.")
        return 0
    cn = pyodbc.connect(dsn, autocommit=True)
    cur = cn.cursor()
    sql = f"""
        INSERT INTO {table}
        (nmId, sizeID, price, discountedPrice, clubDiscountedPrice, discount, clubDiscount, price_rub, discountedPrice_rub, clubDiscountedPrice_rub, discount_total_pct, club_discount_total_pct, spp_pct_approx, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    batch = []
    for r in rows:
        batch.append(
            (
                r.get("nmId"),
                r.get("sizeID"),
                r.get("price"),
                r.get("discountedPrice"),
                r.get("clubDiscountedPrice"),
                r.get("discount"),
                r.get("clubDiscount"),
                r.get("price_rub"),
                r.get("discountedPrice_rub"),
                r.get("clubDiscountedPrice_rub"),
                r.get("discount_total_pct"),
                r.get("club_discount_total_pct"),
                r.get("spp_pct_approx"),
                (r.get("updated_at_utc") or "").replace("Z", "").split("+")[0],
            )
        )
    cur.fast_executemany = True
    step = 1000
    inserted = 0
    for i in range(0, len(batch), step):
        chunk = batch[i : i + step]
        cur.executemany(sql, chunk)
        inserted += len(chunk)
    cur.close()
    cn.close()
    logger.info("Записано строк: %s", inserted)
    return inserted


def write_to_sqlite(
    db_path: str, rows: List[Dict[str, Any]], table: str = "WBGoodsPricesFlat"
) -> int:
    if not rows:
        logger.warning("Нет строк для записи в SQLite — пропускаю.")
        return 0
    p = Path(db_path)
    conn = sqlite3.connect(str(p))
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            nmId TEXT,
            sizeID TEXT,
            price INTEGER,
            discountedPrice INTEGER,
            clubDiscountedPrice INTEGER,
            discount REAL,
            clubDiscount REAL,
            price_rub REAL,
            discountedPrice_rub REAL,
            clubDiscountedPrice_rub REAL,
            discount_total_pct REAL,
            club_discount_total_pct REAL,
            spp_pct_approx REAL,
            updated_at_utc TEXT,
            PRIMARY KEY (nmId, sizeID)
        )
        """
    )
    cur.executemany(
        f"""
        INSERT OR REPLACE INTO {table}
        (nmId, sizeID, price, discountedPrice, clubDiscountedPrice, discount, clubDiscount, price_rub, discountedPrice_rub, clubDiscountedPrice_rub, discount_total_pct, club_discount_total_pct, spp_pct_approx, updated_at_utc)
        VALUES (:nmId, :sizeID, :price, :discountedPrice, :clubDiscountedPrice, :discount, :clubDiscount, :price_rub, :discountedPrice_rub, :clubDiscountedPrice_rub, :discount_total_pct, :club_discount_total_pct, :spp_pct_approx, :updated_at_utc)
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Записано строк в SQLite: %s", len(rows))
    return len(rows)


def import_prices(
    nmids: List[str],
    dsn: str,
    api_key: str | None = None,
    http: requests.Session | None = None,
) -> int:
    http = http or make_http(api_key)
    all_rows: List[Dict[str, Any]] = []
    for i, chunk in enumerate(iter_chunks(nmids, CHUNK_SIZE)):
        filter_nm = ";".join(chunk)
        raw_rows = fetch_batch(
            http, limit=len(chunk), offset=i * CHUNK_SIZE, filter_nm_id=filter_nm
        )
        all_rows.extend(calc_metrics(r) for r in raw_rows)
        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)
    return write_to_db_odbc(all_rows, dsn)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    src_group = parser.add_mutually_exclusive_group(required=True)
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
        "--odbc-table", default="WBGoodsPricesFlat", help="Таблица для записи через ODBC"
    )

    args = parser.parse_args(argv)
    if not (args.out_csv or args.out_sqlite or args.out_odbc):
        parser.error("Нужно указать хотя бы один вывод: --out-csv, --out-sqlite или --out-odbc")
    return args


def main(argv: List[str] | None = None) -> None:
    setup_logging()
    args = parse_args(argv)

    try:
        if args.csv:
            nmids = read_nmids_from_csv(args.csv, args.col)
        elif args.txt:
            nmids = read_nmids_from_txt(args.txt)
        else:
            nmids = read_nmids_from_sqlite(args.sqlite, args.sql)
        if not nmids:
            logger.error("Не найдено ни одного nmId")
            raise SystemExit(1)
        logger.info("Загружено nmId: %s", len(nmids))

        http = make_http(args.api_key)
        rows_out: List[Dict[str, Any]] = []
        for i, chunk in enumerate(iter_chunks(nmids, CHUNK_SIZE)):
            filter_nm = ";".join(chunk)
            try:
                batch = fetch_batch(
                    http,
                    limit=len(chunk),
                    offset=i * CHUNK_SIZE,
                    filter_nm_id=filter_nm,
                )
            except Exception:
                logger.exception("Ошибка при запросе батча nmId: %s", chunk)
                raise SystemExit(1)
            for row in batch:
                rows_out.append(calc_metrics(row))
            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        if args.out_csv:
            write_csv(args.out_csv, rows_out)
        if args.out_sqlite:
            write_to_sqlite(args.out_sqlite, rows_out)
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
