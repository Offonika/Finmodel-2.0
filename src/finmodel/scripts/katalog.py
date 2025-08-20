import sqlite3
import time

import requests

from finmodel.logger import get_logger, setup_logging
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations

# Keep REQUIRED_COLUMNS in sync with ``load_organizations`` implementation.
REQUIRED_COLUMNS = {"id", "Организация", "Token_WB"}

logger = get_logger(__name__)


def main() -> None:
    setup_logging()
    # 📌 Paths
    db_path = get_db_path()

    # 📌 Load organizations
    sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet: %s", sheet)
    logger.info("Using settings sheet %s", settings_sheet)
    df_orgs = load_organizations(sheet=sheet)

    missing_cols = REQUIRED_COLUMNS - set(df_orgs.columns)
    if missing_cols:
        logger.error(
            "Настройки.xlsm is missing required columns: %s",
            ", ".join(sorted(missing_cols)),
        )
        return

    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        return

    # 📌 Подключение к базе
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.cursor()
    except sqlite3.OperationalError as e:
        logger.error("Ошибка подключения к базе: %s", e)
        if conn is not None:
            conn.close()
        raise SystemExit(1)

    # 📌 Пересоздание таблицы с нужными столбцами
    cursor.execute("DROP TABLE IF EXISTS katalog;")
    cursor.execute(
        """
    CREATE TABLE katalog (
        org_id INTEGER,
        Организация TEXT,
        nmID INTEGER,
        imtID INTEGER,
        nmUUID TEXT,
        subjectID INTEGER,
        subjectName TEXT,
        brand TEXT,
        vendorCode TEXT,
        techSize TEXT,
        sku TEXT,
        chrtID INTEGER,
        createdAt TEXT,
        updatedAt TEXT,
        PRIMARY KEY (org_id, chrtID)
    );
    """
    )
    conn.commit()

    # Показываем структуру таблицы
    cursor.execute("PRAGMA table_info(katalog);")
    logger.info("СТРУКТУРА katalog: %s", cursor.fetchall())

    # 📌 Wildberries API
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers_template = {"Content-Type": "application/json"}

    # 📌 Обработка всех организаций
    for _, row in df_orgs.iterrows():
        org_id = row["id"]
        org_name = row["Организация"]
        token = row["Token_WB"]

        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)
        headers = headers_template.copy()
        headers["Authorization"] = token

        has_more = True
        updatedAt = None
        nmID = None

        while has_more:
            payload = {
                "settings": {
                    "cursor": {"limit": 100},
                    "filter": {"withPhoto": -1, "allowedCategoriesOnly": False},
                }
            }

            if updatedAt and nmID:
                payload["settings"]["cursor"].update({"updatedAt": updatedAt, "nmID": nmID})

            try:
                response = requests.post(url, json=payload, headers=headers, timeout=30)
                if response.status_code != 200:
                    logger.warning(
                        "Ошибка запроса: статус %s, ответ: %s", response.status_code, response.text
                    )
                    break
                data = response.json()
            except Exception as e:
                logger.warning("Ошибка запроса: %s", e)
                break

            cards = data.get("cards", [])
            if not cards:
                logger.info("  Нет карточек.")
                break

            rows = []
            for card in cards:
                createdAt = card.get("createdAt")
                updatedAtCard = card.get("updatedAt")
                vendor_code = str(card.get("vendorCode", "")).lower()
                for size in card.get("sizes", []):
                    techSize = size.get("techSize")
                    chrtID = size.get("chrtID")
                    for sku in size.get("skus", []):
                        rows.append(
                            (
                                org_id,
                                org_name,
                                card.get("nmID"),
                                card.get("imtID"),
                                card.get("nmUUID"),
                                card.get("subjectID"),
                                card.get("subjectName"),
                                card.get("brand"),
                                vendor_code,
                                techSize,
                                sku,
                                chrtID,
                                createdAt,
                                updatedAtCard,
                            )
                        )

            if rows:
                try:
                    logger.debug("Writing %s rows to database", len(rows))
                    cursor.executemany(
                        """
                        REPLACE INTO katalog (
                            org_id, Организация, nmID, imtID, nmUUID,
                            subjectID, subjectName, brand, vendorCode,
                            techSize, sku, chrtID, createdAt, updatedAt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        rows,
                    )
                    conn.commit()
                except Exception as e:
                    logger.warning("Ошибка записи в БД: %s", e)
                    break

            # Обновление курсора для следующего запроса
            if cards:
                last_card = cards[-1]
                updatedAt = last_card.get("updatedAt")
                nmID = last_card.get("nmID")
                has_more = len(cards) == 100
                logger.debug("Next cursor: updatedAt=%s, nmID=%s", updatedAt, nmID)
            else:
                has_more = False

            logger.info("  Загружено %s карточек", len(cards))
            if has_more:
                time.sleep(0.6)

    # 📌 Завершение
    conn.close()
    logger.info("✅ Все карточки успешно загружены в таблицу katalog (без дублей).")


if __name__ == "__main__":
    main()
