import argparse
import sqlite3
import time

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger, setup_logging
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations, load_period, parse_date

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Reload data from period start ignoring existing rows",
    )
    args = parser.parse_args(argv or [])

    setup_logging()
    # Максимальный размер страницы, заявленный в документации WB API
    PAGE_LIMIT = 100_000
    REQUEST_TIMEOUT = 60

    # --- Paths ---
    db_path = get_db_path()

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    # --- Получаем период загрузки ---
    period_start_raw, period_end_raw = load_period(sheet=settings_sheet)
    if not period_start_raw or not period_end_raw:
        logger.error("Settings do not include ПериодНачало/ПериодКонец.")
        raise SystemExit(1)
    period_start = parse_date(period_start_raw).strftime("%Y-%m-%dT%H:%M:%S")
    period_end = parse_date(period_end_raw).strftime("%Y-%m-%dT%H:%M:%S")
    logger.info("Период загрузки заказов: %s .. %s", period_start, period_end)

    # --- Load organizations ---
    df_orgs = load_organizations(sheet=org_sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # --- Все возможные поля заказа (WB-API) ---
    ORDER_FIELDS = [
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
        "finishedPrice",
        "priceWithDisc",
        "isCancel",
        "cancelDate",
        "sticker",
        "gNumber",
        "srid",
    ]
    LOWER_FIELDS = {"supplierArticle"}

    # --- Подключение к базе и создание плоской таблицы ---
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in ORDER_FIELDS])
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS OrdersWBFlat (
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
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers_template = {"Content-Type": "application/json"}

    http = make_http()

    for _, row in df_orgs.iterrows():
        org_id = row["id"]
        org_name = row["Организация"]
        token = row["Token_WB"]
        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        headers = headers_template.copy()
        headers["Authorization"] = token

        if args.full_reload:
            cursor.execute(
                "DELETE FROM OrdersWBFlat WHERE org_id = ? AND lastChangeDate >= ?",
                (org_id, period_start),
            )
            conn.commit()
            date_from = period_start
        else:
            cursor.execute(
                """
                SELECT lastChangeDate, srid FROM OrdersWBFlat
                WHERE org_id = ?
                ORDER BY lastChangeDate DESC, srid DESC
                LIMIT 1
                """,
                (org_id,),
            )
            last_row = cursor.fetchone()
            if last_row:
                last_change, last_srid = last_row
                logger.info(
                    "  Last known record lastChangeDate=%s srid=%s",
                    last_change,
                    last_srid,
                )
                date_from = max(last_change, period_start)
            else:
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
                logger.info("✅ Все заказы загружены для этой организации.")
                break

            # Распаковка
            rows = []
            for rec in data:
                flat = [
                    org_id,
                    org_name,
                ] + [
                    str(rec.get(f, "")).lower() if f in LOWER_FIELDS else str(rec.get(f, ""))
                    for f in ORDER_FIELDS
                ]
                rows.append(flat)
            try:
                placeholders = ",".join(["?"] * (2 + len(ORDER_FIELDS)))
                cursor.executemany(
                    f"""
                    INSERT OR REPLACE INTO OrdersWBFlat
                    VALUES ({placeholders})
                """,
                    rows,
                )
                conn.commit()
            except Exception as e:
                logger.warning("  Ошибка вставки: %s", e)
                break

            total_loaded += len(rows)
            logger.info("  +%s заказов (итого: %s)", len(rows), total_loaded)

            if len(rows) < PAGE_LIMIT:
                logger.info("  ✅ Заказы по периоду загружены полностью.")
                break

            # pagination: следующий dateFrom = lastChangeDate последней строки
            date_from = data[-1].get("lastChangeDate")
            page += 1
            time.sleep(3)  # Лимит 1 запрос в минуту, но WB часто разрешает чуть чаще

    conn.close()
    logger.info("✅ Все заказы загружены и распарсены в таблицу OrdersWBFlat (без дублей).")


if __name__ == "__main__":
    main()
