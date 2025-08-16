import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from finmodel.logger import get_logger
from finmodel.utils.settings import load_organizations

logger = get_logger(__name__)


def main() -> None:
    # --- Paths ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = base_dir / "finmodel.db"

    logger.info("DB: %s", db_path)

    # --- Load organizations/tokens ---
    df_orgs = load_organizations()
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        return

    # --- Итоговые поля таблицы ---
    FIELDS = [
        "org_id",
        "Организация",
        "campaignId",
        "campaignName",
        "campaignType",
        "campaignStatus",
        "lastChangeDate",
        "LoadDate",
    ]

    # --- Пересоздаём таблицу ---
    total_rows = 0
    with sqlite3.connect(db_path) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS AdvCampaignsFlat;")
            cursor.execute(
                f"""
    CREATE TABLE AdvCampaignsFlat (
        org_id TEXT,
        Организация TEXT,
        campaignId TEXT,
        campaignName TEXT,
        campaignType TEXT,
        campaignStatus TEXT,
        lastChangeDate TEXT,
        LoadDate TEXT,
        PRIMARY KEY (org_id, campaignId)
    );
    """
            )

            URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
            HEADERS_BASE = {"Content-Type": "application/json"}

            def norm_ts(v):
                if not v or str(v).strip() == "":
                    return ""
                try:
                    return pd.to_datetime(v).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return str(v)

            for _, r in df_orgs.iterrows():
                org_id = str(r["id"])
                org_name = str(r["Организация"])
                token = str(r["Token_WB"]).strip()

                logger.info("→ Организация: %s (ID=%s)", org_name, org_id)
                headers = HEADERS_BASE.copy()
                headers["Authorization"] = token

                try:
                    resp = requests.get(URL, headers=headers, timeout=60)
                    logger.info("  HTTP %s", resp.status_code)
                    preview = (resp.text or "")[:500].replace("\n", " ")
                    logger.info("  Ответ (начало): %s", preview if preview else "[пусто]")

                    if resp.status_code != 200:
                        continue

                    data = resp.json() or {}
                except Exception as e:
                    logger.warning("  Ошибка запроса: %s", e)
                    time.sleep(0.3)
                    continue

                adverts = data.get("adverts", [])
                if not isinstance(adverts, list) or not adverts:
                    logger.warning("  Пустой список adverts.")
                    time.sleep(0.3)
                    continue

                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = []

                # ожидаемый формат: adverts: [{type, status, count, advert_list: [{advertId, changeTime}, ...]}, ...]
                for group in adverts:
                    camp_type = str(group.get("type", ""))  # числовой код типа
                    camp_status = str(group.get("status", ""))  # числовой код статуса
                    items = group.get("advert_list", []) or []
                    for it in items:
                        advert_id = it.get("advertId")
                        change_ts = norm_ts(it.get("changeTime"))
                        if advert_id is None:
                            continue
                        rows.append(
                            [
                                org_id,
                                org_name,
                                str(advert_id),
                                "",  # имя кампании из этого метода не приходит
                                camp_type,
                                camp_status,
                                change_ts,
                                now_str,
                            ]
                        )

                if not rows:
                    logger.warning("  Кампаний не найдено по этому токену.")
                    time.sleep(0.3)
                    continue

                try:
                    placeholders = ",".join(["?"] * len(FIELDS))
                    cursor.executemany(
                        f"INSERT OR REPLACE INTO AdvCampaignsFlat VALUES ({placeholders})", rows
                    )
                    total_rows += len(rows)
                    logger.info("  ✅ Загружено %s кампаний (плоско).", len(rows))
                except Exception as e:
                    logger.warning("  Ошибка вставки: %s", e)

                time.sleep(0.3)  # лимит 5 req/sec

    logger.info("✅ Готово. Всего записей добавлено/обновлено: %s", total_rows)


if __name__ == "__main__":
    main()
