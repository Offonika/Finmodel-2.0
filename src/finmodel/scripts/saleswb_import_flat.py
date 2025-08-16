import json
import sqlite3
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger
from finmodel.utils.settings import (
    find_setting,
    load_organizations,
    load_period,
    parse_date,
)

logger = get_logger(__name__)


def main() -> None:
    # Максимальный размер страницы, заявленный в документации WB API
    PAGE_LIMIT = 100_000
    REQUEST_TIMEOUT = 60

    # --- Paths ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = base_dir / "finmodel.db"

    # --- Получаем период загрузки ---
    period_start_raw, period_end_raw = load_period()
    if not period_start_raw or not period_end_raw:
        logger.error("Settings do not include ПериодНачало/ПериодКонец.")
        raise SystemExit(1)
    period_start = parse_date(period_start_raw).strftime("%Y-%m-%dT%H:%M:%S")
    period_end = parse_date(period_end_raw).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info("Период загрузки продаж: %s .. %s", period_start, period_end)

    # --- Load organizations ---
    sheet = find_setting("ORG_SHEET", default="Настройки")
    logger.info("Using organizations sheet: %s", sheet)
    df_orgs = load_organizations(sheet=sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # --- Все возможные поля продажи (WB-API) ---
    SALES_FIELDS = [
        "date",
        "lastChangeDate",
        "warehouseName",
        "warehouseType",
        "countryName",
        "oblastOkrugName",
        "regionName",
        "supplierArticle",
        "nmId",
        "barcode",
        "category",
        "subject",
        "brand",
        "techSize",
        "incomeID",
        "isSupply",
        "isRealization",
        "totalPrice",
        "discountPercent",
        "spp",
        "paymentSaleAmount",
        "forPay",
        "finishedPrice",
        "priceWithDisc",
        "saleID",
        "sticker",
        "gNumber",
        "srid",
    ]

    # --- Подключение к базе и создание плоской таблицы ---
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in SALES_FIELDS])
    cursor.execute("DROP TABLE IF EXISTS SalesWBFlat;")
    cursor.execute(
        f"""
    CREATE TABLE SalesWBFlat (
        org_id INTEGER,
        Организация TEXT,
        {fields_sql},
        PRIMARY KEY (org_id, srid)
    );
    """
    )
    conn.commit()

    # --- HTTP session ---
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
        s.mount("https://", HTTPAdapter(max_retries=retries))
        s.mount("http://", HTTPAdapter(max_retries=retries))
        return s

    # --- API запрос ---
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers_template = {"Content-Type": "application/json"}

    http = make_http()

    for _, row in df_orgs.iterrows():
        org_id = row["id"]
        org_name = row["Организация"]
        token = row["Token_WB"]
        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        headers = headers_template.copy()
        headers["Authorization"] = token

        date_from = period_start
        total_loaded = 0
        page = 1

        while True:
            params = {"dateFrom": date_from}
            logger.info("  📤 Запрос page %s, dateFrom=%s ...", page, date_from)
            try:
                resp = http.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.status_code != 200:
                    logger.warning("  Запрос вернул статус %s: %s", resp.status_code, resp.text)
                    time.sleep(5)
                    break
                data = resp.json()
            except Exception as e:
                logger.warning("  Ошибка запроса: %s", e)
                time.sleep(5)
                break

            if not data:
                logger.info("✅ Все продажи загружены для этой организации.")
                break

            # Распаковка
            rows = []
            for rec in data:
                flat = [org_id, org_name] + [str(rec.get(f, "")) for f in SALES_FIELDS]
                rows.append(flat)
            try:
                placeholders = ",".join(["?"] * (2 + len(SALES_FIELDS)))
                cursor.executemany(
                    f"""
                    INSERT OR REPLACE INTO SalesWBFlat
                    VALUES ({placeholders})
                """,
                    rows,
                )
                conn.commit()
            except Exception as e:
                logger.warning("  Ошибка вставки: %s", e)
                break

            total_loaded += len(rows)
            logger.info("  +%s продаж (итого: %s)", len(rows), total_loaded)

            if len(rows) < PAGE_LIMIT:
                logger.info("  ✅ Продажи по периоду загружены полностью.")
                break

            # pagination: следующий dateFrom = lastChangeDate последней строки
            date_from = data[-1].get("lastChangeDate")
            page += 1
            time.sleep(3)  # WB лимит: 1 запрос в минуту, но можно чуть чаще

    conn.close()
    logger.info("✅ Все продажи загружены и распарсены в таблицу SalesWBFlat (без дублей).")


if __name__ == "__main__":
    main()
