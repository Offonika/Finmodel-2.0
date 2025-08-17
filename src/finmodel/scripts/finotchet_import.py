import ast
import json
import sqlite3

from finmodel.logger import get_logger
from finmodel.utils.paths import get_db_path

logger = get_logger(__name__)


def main():
    # --- Пути к базе ---
    db_path = get_db_path()

    # --- Список всех WB-полей ---
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

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        try:
            # --- Пересоздаём таблицу для плоских данных ---
            col_defs = (
                "org_id INTEGER, "
                "Организация TEXT, " + ", ".join(f"{f} TEXT" for f in WB_FIELDS) + ", "
                "PRIMARY KEY (org_id, rrd_id)"
            )
            cursor.execute("DROP TABLE IF EXISTS FinOtchetFlat;")
            cursor.execute(f"CREATE TABLE FinOtchetFlat ({col_defs});")

            # --- Загружаем и парсим все строки ---
            cursor.execute("SELECT org_id, Организация, json_data FROM FinOtchet;")
            rows = cursor.fetchall()
            total = 0
            errors = 0

            for org_id, org_name, json_str in rows:
                try:
                    # Сначала пробуем распарсить как JSON, если не получилось — как Python dict через ast.literal_eval
                    try:
                        d = json.loads(json_str)
                    except Exception:
                        d = ast.literal_eval(json_str)
                    values = [org_id, org_name] + [
                        str(d.get(f, "")) if d.get(f) is not None else "" for f in WB_FIELDS
                    ]
                    placeholders = ",".join(["?"] * (2 + len(WB_FIELDS)))
                    cursor.execute(
                        f"INSERT OR REPLACE INTO FinOtchetFlat VALUES ({placeholders})", values
                    )
                    total += 1
                    if total % 1000 == 0:
                        logger.info("  %s строк обработано...", total)
                except Exception as e:
                    logger.warning("Ошибка парсинга/org_id=%s: %s", org_id, e)
                    errors += 1

            logger.info("✅ Всего записей обработано и вставлено: %s", total)
            if errors:
                logger.warning("Были ошибки парсинга: %s записей не обработаны", errors)
        finally:
            cursor.close()

    logger.info("Готово! Таблица FinOtchetFlat содержит плоские данные для PowerBI/Excel.")


if __name__ == "__main__":
    main()
