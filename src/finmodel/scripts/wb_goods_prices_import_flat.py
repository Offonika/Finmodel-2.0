"""Fetch Wildberries goods prices and store them in CSV/SQLite/ODBC."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger, setup_logging

WB_ENDPOINT = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
CHUNK_SIZE = 100
TIMEOUT = 15
SLEEP_BETWEEN_BATCHES_SEC = 0.4

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
    s.headers.update(
        {"Accept": "application/json", "User-Agent": "WB-SPP-Fetcher/1.0 (+PowerBI)"}
    )
    if api_key:
        s.headers["Authorization"] = api_key
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def fetch_batch(
    http: requests.Session, limit: int, offset: int, filter_nm_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Возвращает список словарей в унифицированном формате:
    nmId, sizeID, priceU, salePriceU, sale
    """
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if filter_nm_id:
        params["filterNmID"] = filter_nm_id

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
        nm_id_raw: Any = p.get("nmId") or p.get("nmID") or p.get("id")
        nm_id: Optional[str] = str(nm_id_raw) if nm_id_raw is not None else None
        sizes_any: Any = p.get("sizes") or []
        sizes: List[Dict[str, Any]] = sizes_any if isinstance(sizes_any, list) else []

        # Если размеров нет — берём цену с уровня товара (если она там есть)
        if not sizes:
            out.append(
                {
                    "nmId": nm_id,
                    "sizeID": None,
                    "priceU": p.get("priceU") if isinstance(p.get("priceU"), int) else None,
                    "salePriceU": (
                        p.get("salePriceU") if isinstance(p.get("salePriceU"), int) else None
                    ),
                    "sale": (
                        float(p.get("sale")) if isinstance(p.get("sale"), (int, float)) else None
                    ),
                }
            )
            continue

        for s in sizes:
            # поля могут быть на уровне size, а могут наследоваться от товара
            priceU: Optional[int] = s.get("priceU") if isinstance(s.get("priceU"), int) else (
                p.get("priceU") if isinstance(p.get("priceU"), int) else None
            )
            salePriceU: Optional[int] = (
                s.get("salePriceU") if isinstance(s.get("salePriceU"), int) else (
                    p.get("salePriceU") if isinstance(p.get("salePriceU"), int) else None
                )
            )
            sale_val: Any = s.get("sale")
            if sale_val is None:
                sale_val = p.get("sale")
            sale: Optional[float] = float(sale_val) if isinstance(sale_val, (int, float)) else None

            size_raw: Any = s.get("sizeId") or s.get("sizeID") or s.get("id")
            size_id: Optional[str] = str(size_raw) if size_raw is not None else None

            out.append(
                {
                    "nmId": nm_id,
                    "sizeID": size_id,
                    "priceU": priceU,
                    "salePriceU": salePriceU,
                    "sale": sale,
                }
            )
    return out


def calc_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    priceU: Optional[int] = row.get("priceU")
    salePriceU: Optional[int] = row.get("salePriceU")
    sale: Optional[float] = row.get("sale")

    discount_total_pct: Optional[float] = (
        (1 - (salePriceU / priceU)) * 100.0
        if priceU is not None and salePriceU is not None and priceU != 0
        else None
    )
    # Примерная оценка «СПП» как разница между полной скидкой и публичной скидкой WB
    spp_pct_approx: Optional[float] = (
        discount_total_pct - sale if discount_total_pct is not None and sale is not None else None
    )

    return {
        **row,
        "price_rub": (priceU / 100.0) if isinstance(priceU, int) else None,
        "salePrice_rub": (salePriceU / 100.0) if isinstance(salePriceU, int) else None,
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


# ───────────────────────────── IO: sinks ───────────────────────────── #

CSV_FIELDS: List[str] = [
    "nmId",
    "sizeID",
    "priceU",
    "salePriceU",
    "sale",
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
        # Создаём таблицу, если отсутствует
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                nmId TEXT,
                sizeID TEXT,
                priceU INTEGER,
                salePriceU INTEGER,
                sale REAL,
                price_rub REAL,
                salePrice_rub REAL,
                discount_total_pct REAL,
                spp_pct_approx REAL,
                updated_at_utc TEXT,
                PRIMARY KEY (nmId, sizeID)
            )
            """
        )
        # Подготовим данные к вставке (позиционные параметры)
        data: List[Tuple[Any, ...]] = [
            (
                r.get("nmId"),
                r.get("sizeID"),
                r.get("priceU"),
                r.get("salePriceU"),
                r.get("sale"),
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
            (nmId, sizeID, priceU, salePriceU, sale, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        conn.commit()
    logger.info("Записано строк в SQLite: %s", len(rows))
    return len(rows)


def write_to_db_odbc(rows: List[Dict[str, Any]], dsn: str, table: str = "dbo.WBGoodsPricesFlat") -> int:
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
        # Очистить таблицу (если нужна полная замена — по желанию можно убрать)
        cur.execute(f"TRUNCATE TABLE {table}")
        # Подготовка данных
        data: List[Tuple[Any, ...]] = [
            (
                r.get("nmId"),
                r.get("sizeID"),
                r.get("priceU"),
                r.get("salePriceU"),
                r.get("sale"),
                r.get("price_rub"),
                r.get("salePrice_rub"),
                r.get("discount_total_pct"),
                r.get("spp_pct_approx"),
                r.get("updated_at_utc"),
            )
            for r in rows
        ]
        # Вставка
        cur.fast_executemany = True
        cur.executemany(
            f"""
            INSERT INTO {table}
            (nmId, sizeID, priceU, salePriceU, sale, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
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
    nmids: List[str],
    dsn: Optional[str],
    api_key: Optional[str] = None,
    http: Optional[requests.Session] = None,
) -> int:
    http = http or make_http(api_key)
    all_rows: List[Dict[str, Any]] = []
    for i, chunk in enumerate(iter_chunks(nmids, CHUNK_SIZE)):
        filter_nm = ";".join(chunk)
        raw_rows = fetch_batch(http, limit=len(chunk), offset=i * CHUNK_SIZE, filter_nm_id=filter_nm)
        all_rows.extend(calc_metrics(r) for r in raw_rows)
        time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

    written: int = 0
    if dsn:
        written = write_to_db_odbc(all_rows, dsn)
    return written


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
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
        "--odbc-table", default="dbo.WBGoodsPricesFlat", help="Таблица для записи через ODBC"
    )

    args = parser.parse_args(argv)
    if not (args.out_csv or args.out_sqlite or args.out_odbc):
        parser.error("Нужно указать хотя бы один вывод: --out-csv, --out-sqlite или --out-odbc")
    return args


def main(argv: Optional[List[str]] = None) -> None:
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
