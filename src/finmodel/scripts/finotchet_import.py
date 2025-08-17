import sqlite3
import time

import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.logger import get_logger, setup_logging
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import (
    find_setting,
    load_organizations,
    load_period,
    parse_date,
)

logger = get_logger(__name__)


WB_FIELDS = [
    "realizationreport_id",
    "date_from",
    "date_to",
    "create_dt",
    "currency_name",
    "suppliercontract_code",
    "rrd_id",
    "gi_id",
    "dlv_prc",
    "fix_tariff_date_from",
    "fix_tariff_date_to",
    "subject_name",
    "nm_id",
    "brand_name",
    "sa_name",
    "ts_name",
    "barcode",
    "doc_type_name",
    "quantity",
    "retail_price",
    "retail_amount",
    "sale_percent",
    "commission_percent",
    "office_name",
    "supplier_oper_name",
    "order_dt",
    "sale_dt",
    "rr_dt",
    "shk_id",
    "retail_price_withdisc_rub",
    "delivery_amount",
    "return_amount",
    "delivery_rub",
    "gi_box_type_name",
    "product_discount_for_report",
    "supplier_promo",
    "ppvz_spp_prc",
    "ppvz_kvw_prc_base",
    "ppvz_kvw_prc",
    "sup_rating_prc_up",
    "is_kgvp_v2",
    "ppvz_sales_commission",
    "ppvz_for_pay",
    "ppvz_reward",
    "acquiring_fee",
    "acquiring_percent",
    "payment_processing",
    "acquiring_bank",
    "ppvz_vw",
    "ppvz_vw_nds",
    "ppvz_office_name",
    "ppvz_office_id",
    "ppvz_supplier_id",
    "ppvz_supplier_name",
    "ppvz_inn",
    "declaration_number",
    "bonus_type_name",
    "sticker_id",
    "site_country",
    "srv_dbs",
    "penalty",
    "additional_payment",
    "rebill_logistic_cost",
    "rebill_logistic_org",
    "storage_fee",
    "deduction",
    "acceptance",
    "assembly_id",
    "kiz",
    "srid",
    "report_type",
    "is_legal_entity",
    "trbx_id",
    "installment_cofinancing_amount",
    "wibes_wb_discount_percent",
    "cashback_amount",
    "cashback_discount",
]


def make_http() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def main() -> None:
    setup_logging()

    PAGE_LIMIT = 100_000
    REQUEST_TIMEOUT = 60
    API_SLEEP = 60

    db_path = get_db_path()

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    period_start_raw, period_end_raw = load_period(sheet=settings_sheet)
    if not period_start_raw or not period_end_raw:
        logger.error("Settings do not include ПериодНачало/ПериодКонец.")
        raise SystemExit(1)
    date_from = parse_date(period_start_raw).strftime("%Y-%m-%d")
    date_to = parse_date(period_end_raw).strftime("%Y-%m-%d")
    logger.info("Период загрузки фин. отчёта: %s .. %s", date_from, date_to)

    df_orgs = load_organizations(sheet=org_sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in WB_FIELDS])
    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS FinOtchet (
        org_id INTEGER,
        Организация TEXT,
        {fields_sql},
        PRIMARY KEY (org_id, rrd_id)
    );
    """
    )
    conn.commit()

    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers_template = {"Content-Type": "application/json"}
    http = make_http()

    for _, row in df_orgs.iterrows():
        org_id = row["id"]
        org_name = row["Организация"]
        token = row["Token_WB"]
        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        headers = headers_template.copy()
        headers["Authorization"] = token

        rrdid = 0
        total_loaded = 0
        page = 1

        while True:
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "rrdid": rrdid,
                "limit": PAGE_LIMIT,
            }
            logger.info("  📤 Запрос page %s, rrdid=%s ...", page, rrdid)
            try:
                resp = http.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
                if resp.status_code != 200:
                    logger.warning("  Запрос вернул статус %s: %s", resp.status_code, resp.text)
                    time.sleep(API_SLEEP)
                    break
                data = resp.json()
            except Exception as e:
                logger.warning("  Ошибка запроса: %s", e)
                time.sleep(API_SLEEP)
                break

            if not data:
                logger.info("✅ Фин. отчёт загружен для этой организации.")
                break

            rows = []
            for rec in data:
                rows.append([org_id, org_name] + [str(rec.get(f, "")) for f in WB_FIELDS])

            try:
                placeholders = ",".join(["?"] * (2 + len(WB_FIELDS)))
                cursor.executemany(
                    f"INSERT OR REPLACE INTO FinOtchet VALUES ({placeholders})",
                    rows,
                )
                conn.commit()
            except Exception as e:
                logger.warning("  Ошибка вставки: %s", e)
                break

            total_loaded += len(rows)
            logger.info("  +%s записей (итого: %s)", len(rows), total_loaded)

            if len(rows) < PAGE_LIMIT:
                logger.info("  ✅ Отчёт по периоду загружен полностью.")
                break

            rrdid = int(data[-1].get("rrd_id", 0))
            page += 1
            time.sleep(API_SLEEP)

    conn.close()
    logger.info("✅ Все отчёты загружены в таблицу FinOtchet.")


if __name__ == "__main__":
    main()
