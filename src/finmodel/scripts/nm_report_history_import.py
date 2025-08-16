import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

from finmodel.logger import get_logger
from finmodel.utils.settings import find_setting, load_organizations

logger = get_logger(__name__)


def main() -> None:
    # --- Paths ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = base_dir / "finmodel.db"

    logger.info("DB: %s", db_path)

    # --- Период: всегда последние 7 дней (включая сегодня) ---
    today = datetime.now().date()
    date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")
    logger.info("Период запроса: %s .. %s", date_from, date_to)

    # --- Load organizations and tokens ---
    sheet = find_setting("ORG_SHEET", default="Настройки")
    logger.info("Using organizations sheet: %s", sheet)
    df_orgs = load_organizations(sheet=sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # --- Подключение к БД ---
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # --- Таблица результата (плоская) ---
    TABLE = "WB_NMReportHistory"
    cur.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        org_id TEXT,
        Организация TEXT,
        nmID TEXT,
        imtName TEXT,
        vendorCode TEXT,
        dt TEXT,
        openCardCount TEXT,
        addToCartCount TEXT,
        ordersCount TEXT,
        ordersSumRub TEXT,
        buyoutsCount TEXT,
        buyoutsSumRub TEXT,
        buyoutPercent TEXT,
        addToCartConversion TEXT,
        cartToOrderConversion TEXT,
        LoadDate TEXT,
        PRIMARY KEY (org_id, nmID, dt)
    );
    """
    )
    conn.commit()

    # --- Хелперы ---
    def chunked(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    HEADERS_BASE = {"Content-Type": "application/json"}
    SLEEP_BETWEEN_CALLS = 20  # лимит 3 req/min → держим 20 сек

    def get_nmids_for_org(c, org_id, org_name):
        # 1) katalog
        try:
            rows = c.execute(
                "SELECT DISTINCT nmID FROM katalog WHERE org_id = ? AND nmID IS NOT NULL", (org_id,)
            ).fetchall()
            nmids = [int(r[0]) for r in rows if str(r[0]).strip() != ""]
            if nmids:
                return sorted(set(nmids))
        except Exception:
            pass
        # 2) WBGoodsPricesFlat
        try:
            rows = c.execute(
                "SELECT DISTINCT nmID FROM WBGoodsPricesFlat WHERE org_id = ? AND nmID IS NOT NULL",
                (org_id,),
            ).fetchall()
            nmids = [int(r[0]) for r in rows if str(r[0]).strip() != ""]
            if nmids:
                return sorted(set(nmids))
        except Exception:
            pass
        logger.warning("  Не найдены nmID для организации %s (ID=%s).", org_name, org_id)
        return []

    def do_request(token, nm_ids):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        body = {
            "nmIDs": nm_ids,  # максимум 20
            "period": {"begin": date_from, "end": date_to},
            "timezone": "Europe/Moscow",
            "aggregationLevel": "day",
        }
        resp = requests.post(API_URL, headers=headers, json=body, timeout=90)
        return resp

    # --- Основной цикл по организациям ---
    total_inserted = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, org in df_orgs.iterrows():
        org_id = str(org["id"])
        org_name = str(org["Организация"])
        token = str(org["Token_WB"]).strip()

        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)
        nmids = get_nmids_for_org(cur, org_id, org_name)
        if not nmids:
            continue

        logger.info("  Всего nmID: %s (батчи по 20)", len(nmids))

        batch_num = 0
        for batch in chunked(nmids, 20):
            batch_num += 1
            logger.info("  ▶ Батч %s: %s nmID", batch_num, len(batch))

            try:
                resp = do_request(token, batch)
                # Если упёрлись в лимит — немного подождём и повторим 1 раз
                if resp.status_code == 429:
                    logger.warning("    429 Too Many Requests. Жду 25 сек и повторяю…")
                    time.sleep(25)
                    resp = do_request(token, batch)

                if resp.status_code == 401:
                    logger.error(
                        "    401 Unauthorized — проверьте токен/права (нужна аналитика). Пропускаю организацию."
                    )
                    break

                if resp.status_code != 200:
                    logger.warning("    HTTP %s: %s", resp.status_code, resp.text[:300])
                    # даже при ошибке соблюдём паузу между вызовами
                    time.sleep(SLEEP_BETWEEN_CALLS)
                    continue

                payload = resp.json() or {}
                data = payload.get("data", [])
                if not isinstance(data, list):
                    logger.warning("    Неожиданный формат ответа (ожидали массив 'data').")
                    time.sleep(SLEEP_BETWEEN_CALLS)
                    continue

                rows = []
                for item in data:
                    nm = str(item.get("nmID", ""))
                    imtName = str(item.get("imtName", ""))
                    vendorCode = str(item.get("vendorCode", ""))
                    history = item.get("history", []) or []
                    for h in history:
                        rows.append(
                            [
                                org_id,
                                org_name,
                                nm,
                                imtName,
                                vendorCode,
                                str(h.get("dt", "")),
                                str(h.get("openCardCount", "")),
                                str(h.get("addToCartCount", "")),
                                str(h.get("ordersCount", "")),
                                str(h.get("ordersSumRub", "")),
                                str(h.get("buyoutsCount", "")),
                                str(h.get("buyoutsSumRub", "")),
                                str(h.get("buyoutPercent", "")),
                                str(h.get("addToCartConversion", "")),
                                str(h.get("cartToOrderConversion", "")),
                                now_str,
                            ]
                        )

                if rows:
                    ph = ",".join(["?"] * 16)
                    cur.executemany(f"INSERT OR REPLACE INTO {TABLE} VALUES ({ph})", rows)
                    conn.commit()
                    total_inserted += len(rows)
                    logger.info("    ✅ +%s строк (итого: %s)", len(rows), total_inserted)
                else:
                    logger.warning("    Пустые данные по этому батчу.")

            except Exception as e:
                logger.warning("    Ошибка запроса/вставки: %s", e)

            # строго выдерживаем лимит 3 запроса/мин → пауза 20 секунд
            time.sleep(SLEEP_BETWEEN_CALLS)

    conn.close()
    logger.info("✅ Готово. Всего добавлено/обновлено строк: %s в %s", total_inserted, TABLE)


if __name__ == "__main__":
    main()
