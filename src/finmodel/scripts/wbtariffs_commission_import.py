import sqlite3

import requests

from finmodel.logger import get_logger
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations

logger = get_logger(__name__)


def main() -> None:
    # --- Paths ---
    db_path = get_db_path()

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    # --- Load all tokens ---
    df_orgs = load_organizations(sheet=org_sheet)
    tokens = df_orgs["Token_WB"].dropna().astype(str).tolist()
    if not tokens:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        return

    # --- Поля по документации WB ---
    FIELDS = [
        "kgvpBooking",
        "kgvpMarketplace",
        "kgvpPickup",
        "kgvpSupplier",
        "kgvpSupplierExpress",
        "paidStorageKgvp",
        "parentID",
        "parentName",
        "subjectID",
        "subjectName",
    ]

    # --- Пересоздаём таблицу ---
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in FIELDS])
    cursor.execute("DROP TABLE IF EXISTS WBTariffsCommission;")
    cursor.execute(
        f"""
    CREATE TABLE WBTariffsCommission (
        {fields_sql}
    );
    """
    )
    conn.commit()

    # --- Пытаемся получить комиссии с каждым токеном ---
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    params = {"locale": "ru"}
    found_data = False

    for idx, token in enumerate(tokens, 1):
        headers = {"Authorization": token}
        logger.info("Пробую токен №%s ...", idx)
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            if resp.status_code != 200:
                logger.warning("ответ WB: %s", resp.status_code)
                continue
            data = resp.json().get("report", [])
            if not data:
                logger.info("данных нет.")
                continue
            logger.info("Успех!")
            # --- Вставляем плоско ---
            rows = []
            for rec in data:
                flat = [str(rec.get(f, "")) for f in FIELDS]
                rows.append(flat)
            placeholders = ",".join(["?"] * len(FIELDS))
            cursor.executemany(
                f"""
                INSERT INTO WBTariffsCommission
                VALUES ({placeholders})
            """,
                rows,
            )
            conn.commit()
            logger.info("Вставлено %s строк в таблицу WBTariffsCommission", len(rows))
            found_data = True
            break
        except Exception as e:
            logger.warning("Ошибка запроса: %s", e)
            continue

    if not found_data:
        logger.error(
            "Не удалось получить данные ни с одним токеном. Проверьте права или интернет/домен WB."
        )

    conn.close()
    logger.info("✅ Комиссии по категориям WB: загрузка завершена.")


if __name__ == "__main__":
    main()
