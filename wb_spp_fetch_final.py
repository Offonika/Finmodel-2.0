# file: wb_spp_fetch_final.py
from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

# ----- WB endpoints -----
ENDPOINTS = [
    "https://card.wb.ru/cards/v4/detail",
    "https://card.wb.ru/cards/v2/detail",
    "https://card.wb.ru/cards/detail",
    "https://card.wb.ru/cards/v1/detail",
]

# ----- defaults (витрина/локаль — из твоего DevTools) -----
DEFAULT_DEST = 12358062
DEFAULT_SPP = 30
DEFAULT_HIDE_DTYPE = None
DEFAULT_AB_TESTID = "no_reranking"
DEFAULT_LANG = "ru"

DEFAULT_APP_TYPE = 1
DEFAULT_CURR = "rub"

DEFAULT_CHUNK = 100
DEFAULT_SLEEP = 0.5
DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES = 5

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SQLITE_PATH = (SCRIPT_DIR.parent / "finmodel.db").as_posix()
DEFAULT_SQLITE_TABLE = "wb_spp"
LOG_PATH = SCRIPT_DIR / "log" / "wb_spp_fetch_final.log"

# ----- logging -----
def setup_logging():
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logging.info("Логи → %s", LOG_PATH)

# ----- SQLite -----
def read_nmids_from_sqlite(db_path: str, sql: Optional[str]) -> List[str]:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"SQLite файл не найден: {p}")
    with sqlite3.connect(str(p)) as con:
        con.row_factory = sqlite3.Row
        with con.cursor() as cur:
            if sql:
                rows = cur.execute(sql).fetchall()
            else:
                rows = []
                for q in (
                    "SELECT DISTINCT nmId AS nmId FROM katalog WHERE nmId IS NOT NULL",
                    "SELECT DISTINCT nm_id AS nmId FROM katalog WHERE nm_id IS NOT NULL",
                ):
                    try:
                        rows = cur.execute(q).fetchall()
                        if rows:
                            break
                    except sqlite3.Error:
                        continue
                if not rows:
                    raise SystemExit(
                        "Не найдено поле nmId. Задай SQL через --sql, напр.:\n"
                        '  --sql "SELECT DISTINCT nm_id AS nmId FROM katalog WHERE nm_id IS NOT NULL"'
                    )
    nmids = [str(r["nmId"]).strip() for r in rows if r["nmId"] not in (None, "")]
    return nmids

def sqlite_ensure_table(con: sqlite3.Connection, table: str):
    with con.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                nmId TEXT PRIMARY KEY,
                priceU INTEGER,
                salePriceU INTEGER,
                sale REAL,
                price_rub REAL,
                salePrice_rub REAL,
                discount_total_pct REAL,
                spp_pct_approx REAL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_nmId ON {table}(nmId)")

def write_to_sqlite(db_path: str, table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        logging.info("Нет строк для записи в SQLite — пропуск.")
        return
    with sqlite3.connect(db_path) as con:
        sqlite_ensure_table(con, table)
        with con.cursor() as cur:
            sql = f"""
                INSERT INTO {table}
                (nmId, priceU, salePriceU, sale, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(nmId) DO UPDATE SET
                    priceU=excluded.priceU,
                    salePriceU=excluded.salePriceU,
                    sale=excluded.sale,
                    price_rub=excluded.price_rub,
                    salePrice_rub=excluded.salePrice_rub,
                    discount_total_pct=excluded.discount_total_pct,
                    spp_pct_approx=excluded.spp_pct_approx,
                    updated_at_utc=excluded.updated_at_utc
            """
            data = [(
                r.get("nmId"),
                r.get("priceU"),
                r.get("salePriceU"),
                r.get("sale"),
                r.get("price_rub"),
                r.get("salePrice_rub"),
                r.get("discount_total_pct"),
                r.get("spp_pct_approx"),
                r.get("updated_at_utc"),
            ) for r in rows]
            cur.executemany(sql, data)
    logging.info("Данные записаны в SQLite: %s (%d строк)", table, len(rows))

# ----- HTTP / WB -----
def make_http(timeout: int, retries: int) -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    s.headers.update({"Accept": "application/json", "User-Agent": "WB-SPP-Fetcher/Final (+Finmodel2.0)"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    s.request_timeout = timeout
    return s

def _request_cards(http: requests.Session, endpoint: str, params: dict, headers: dict, timeout: int) -> dict:
    resp = http.get(endpoint, params=params, headers=headers, timeout=timeout)
    if resp.status_code == 404:
        raise requests.HTTPError("404", response=resp)
    resp.raise_for_status()
    return resp.json()

def _parse_rows(payload: dict) -> List[Dict[str, Any]]:
    products = (payload or {}).get("data", {}).get("products", []) or []
    out: List[Dict[str, Any]] = []
    for p in products:
        out.append({
            "nmId": str(p.get("id")) if p.get("id") is not None else None,
            "priceU": p.get("priceU") if isinstance(p.get("priceU"), int) else None,
            "salePriceU": p.get("salePriceU") if isinstance(p.get("salePriceU"), int) else None,
            "sale": float(p.get("sale")) if isinstance(p.get("sale"), (int, float)) else None,
        })
    return out

def fetch_batch(http: requests.Session, ids: List[str], caps: Dict[str, Any], timeout: int) -> List[Dict[str, Any]]:
    ids = [s for s in (str(x).strip() for x in ids) if s]
    if not ids:
        return []
    params = {
        "appType": caps.get("appType", DEFAULT_APP_TYPE),
        "curr": caps.get("curr", DEFAULT_CURR),
        "dest": caps["dest"],
        "nm": ";".join(ids),
        "lang": caps.get("lang", DEFAULT_LANG),
    }
    if caps.get("hide_dtype") not in (None, 0):
        params["spp"] = caps["spp"]
    if caps.get("hide_dtype") is not None:
        params["hide_dtype"] = caps["hide_dtype"]
    if caps.get("ab_testid"):
        params["ab_testid"] = caps["ab_testid"]

    headers = {"Accept": "application/json", "Accept-Language": "ru-RU,ru;q=0.9"}
    if caps.get("origin"):
        headers["Origin"] = caps["origin"]
    if caps.get("referer"):
        headers["Referer"] = caps["referer"]
    if caps.get("captcha_id"):
        headers["x-captcha-id"] = caps["captcha_id"]

    # v4 → v2 → legacy → v1
    for ep in ENDPOINTS:
        try:
            payload = _request_cards(http, ep, params, headers, timeout)
            return _parse_rows(payload)
        except requests.HTTPError as e:
            if getattr(e, "response", None) is not None and e.response.status_code == 404:
                continue
            raise

    # все EP вернули 404 → дробим пачку
    if len(ids) == 1:
        logging.warning("Пропускаю nmId=%s (API 404 на всех эндпоинтах).", ids[0])
        return []
    mid = len(ids) // 2
    left = fetch_batch(http, ids[:mid], caps, timeout)
    right = fetch_batch(http, ids[mid:], caps, timeout)
    return left + right

# ----- metrics -----
def calc_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    price_u = row.get("priceU")
    sale_price_u = row.get("salePriceU")
    sale = row.get("sale")
    if isinstance(price_u, int) and isinstance(sale_price_u, int) and price_u:
        discount_total_pct = (1 - (sale_price_u / price_u)) * 100.0
    else:
        discount_total_pct = None
    spp_pct_approx = (discount_total_pct - sale) if (discount_total_pct is not None and sale is not None) else None
    return {
        **row,
        "price_rub": (price_u / 100.0) if isinstance(price_u, int) else None,
        "salePrice_rub": (sale_price_u / 100.0) if isinstance(sale_price_u, int) else None,
        "discount_total_pct": round(discount_total_pct, 2) if isinstance(discount_total_pct, float) else None,
        "spp_pct_approx": round(spp_pct_approx, 2) if isinstance(spp_pct_approx, float) else None,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

# ----- main -----
def main():
    setup_logging()
    ap = argparse.ArgumentParser(description="WB SPP → SQLite (без браузера).")
    ap.add_argument("--sqlite", default=DEFAULT_SQLITE_PATH)
    ap.add_argument("--sql", help="SQL, который вернёт столбец nmId.")
    ap.add_argument("--table", default=DEFAULT_SQLITE_TABLE)
    ap.add_argument("--dest", type=int, default=DEFAULT_DEST)
    ap.add_argument("--spp", type=int, default=DEFAULT_SPP)
  
    ap.add_argument("--hide-dtype", type=int, dest="hide_dtype",
                help="Скрывать товары указанного dtype (по умолчанию — не скрывать)")
    ap.add_argument("--ab-testid", dest="ab_testid", default=DEFAULT_AB_TESTID)
    ap.add_argument("--lang", default=DEFAULT_LANG)
    ap.add_argument("--origin", default="https://www.wildberries.ru")
    ap.add_argument("--referer")
    ap.add_argument("--captcha-id", dest="captcha_id")
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK)
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    ap.add_argument("--test-nmid", help="Парсить только один nmId (игнорирует --sql).")
    args = ap.parse_args()

    logging.info("SQLite: %s; table: %s", args.sqlite, args.table)

    # nmId список
    if args.test_nmid:
        nmids = [str(args.test_nmid)]
        logging.info("Режим теста, nmId=%s", nmids[0])
    else:
        nmids = read_nmids_from_sqlite(args.sqlite, args.sql)
        nmids = list(dict.fromkeys(nmids))
        logging.info("Всего nmId к обработке: %d", len(nmids))

    caps = {
        "dest": args.dest,
        "spp": args.spp,
        "hide_dtype": args.hide_dtype,
        "ab_testid": args.ab_testid,
        "lang": args.lang,
        "origin": args.origin,
        "referer": args.referer,
        "captcha_id": args.captcha_id,
        "appType": DEFAULT_APP_TYPE,
        "curr": DEFAULT_CURR,
    }
    logging.info("Параметры запроса: %s", caps)

    http = make_http(args.timeout, args.retries)

    total_batches = math.ceil(len(nmids) / args.chunk)
    requested = set(nmids)
    received: set[str] = set()
    all_rows: List[Dict[str, Any]] = []

    for i in range(total_batches):
        part = nmids[i * args.chunk : (i + 1) * args.chunk]
        try:
            rows = fetch_batch(http, part, caps, args.timeout)
            rows = [calc_metrics(r) for r in rows]
            all_rows.extend(rows)
            received.update([r.get("nmId") for r in rows if r.get("nmId")])
            logging.info("Батч %d/%d: запрошено=%d → получено=%d", i + 1, total_batches, len(part), len(rows))
        except Exception as e:
            logging.error("Ошибка в батче %d: %s", i + 1, e)
        time.sleep(args.sleep)

    missing = sorted(requested - received)
    if missing:

        logging.warning("WB не вернул %d nmId. Примеры: %s%s",
                        len(missing), ", ".join(missing[:10]), " ..." if len(missing) > 10 else "")

    write_to_sqlite(args.sqlite, args.table, all_rows)
    logging.info("Готово.")

if __name__ == "__main__":
    main()
