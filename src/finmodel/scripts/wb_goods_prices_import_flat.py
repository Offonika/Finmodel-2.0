def main():
    # file: wb_spp_fetch.py  (v2)
    import csv

    # --- NEW: sqlite ---
    import sqlite3
    import sys
    import time
    from datetime import datetime, timezone
    from pathlib import Path
    from typing import Any, Dict, Iterable, List

    import requests
    from requests.adapters import HTTPAdapter, Retry

    from finmodel.logger import get_logger

    WB_ENDPOINT = "https://card.wb.ru/cards/v1/detail"
    CHUNK_SIZE = 100
    TIMEOUT = 15
    SLEEP_BETWEEN_BATCHES_SEC = 0.4

    def iter_chunks(lst: List[str], size: int) -> Iterable[List[str]]:
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    def make_http() -> requests.Session:
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
        s.mount("https://", HTTPAdapter(max_retries=retries))
        s.mount("http://", HTTPAdapter(max_retries=retries))
        return s

    logger = get_logger(__name__)

    def fetch_batch(http: requests.Session, ids: List[str]) -> List[Dict[str, Any]]:
        ids_clean = [str(x).strip() for x in ids if str(x).strip()]
        if not ids_clean:
            return []
        params = {"appType": 1, "curr": "rub", "dest": -1257786, "nm": ";".join(ids_clean)}
        r = http.get(WB_ENDPOINT, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        products = (payload or {}).get("data", {}).get("products", []) or []

        out = []
        for p in products:
            out.append(
                {
                    "nmId": str(p.get("id")) if p.get("id") is not None else None,
                    "priceU": p.get("priceU") if isinstance(p.get("priceU"), int) else None,
                    "salePriceU": (
                        p.get("salePriceU") if isinstance(p.get("salePriceU"), int) else None
                    ),
                    "sale": (
                        float(p.get("sale")) if isinstance(p.get("sale"), (int, float)) else None
                    ),
                }
            )
        return out

    def calc_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
        price_u = row.get("priceU")
        sale_price_u = row.get("salePriceU")
        sale = row.get("sale")

        discount_total_pct = (
            (1 - (sale_price_u / price_u)) * 100.0
            if price_u and sale_price_u and price_u != 0
            else None
        )
        spp_pct_approx = (
            (discount_total_pct - sale)
            if (discount_total_pct is not None and sale is not None)
            else None
        )

        return {
            **row,
            "price_rub": (price_u / 100.0) if isinstance(price_u, int) else None,
            "salePrice_rub": (sale_price_u / 100.0) if isinstance(sale_price_u, int) else None,
            "discount_total_pct": discount_total_pct,
            "spp_pct_approx": spp_pct_approx,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    def read_nmids_from_csv(path: str, col: str) -> List[str]:
        import csv

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

    # --- NEW: читать nmId из SQLite ---
    def read_nmids_from_sqlite(db_path: str, sql: str | None) -> List[str]:
        p = Path(db_path)
        if not p.exists():
            raise SystemExit(f"SQLite файл не найден: {p}")

        rows: list[sqlite3.Row] = []
        with sqlite3.connect(str(p)) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                # Если SQL не задан, пробуем два стандартных варианта
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
            "priceU",
            "salePriceU",
            "sale",
            "price_rub",
            "salePrice_rub",
            "discount_total_pct",
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
            (nmId, priceU, salePriceU, sale, price_rub, salePrice_rub, discount_total_pct, spp_pct_approx, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        batch = []
        for r in rows:
            batch.append(
                (
                    r.get("nmId"),
                    r.get("priceU"),
                    r.get("salePriceU"),
                    r.get("sale"),
                    r.get("price_rub"),
                    r.get("salePrice_rub"),
                    r.get("discount_total_pct"),
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


if __name__ == "__main__":
    main()
