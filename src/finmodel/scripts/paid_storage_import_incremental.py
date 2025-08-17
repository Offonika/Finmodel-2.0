import sqlite3
import time
from datetime import date, datetime, timedelta

import requests

from finmodel.logger import get_logger
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations

logger = get_logger(__name__)


def main() -> None:
    # ---------- Paths ----------
    db_path = get_db_path()

    logger.info("DB: %s", db_path)

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    # ---------- Helpers ----------
    def iso_date(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    def daterange_8d(d1: date, d2: date):
        cur = d1
        while cur <= d2:
            end = min(cur + timedelta(days=7), d2)
            yield cur, end
            cur = end + timedelta(days=1)

    def sleep_log(sec: float, msg=""):
        if msg:
            logger.info(msg)
        time.sleep(sec)

    # ---------- Orgs ----------
    df_orgs = load_organizations(sheet=org_sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # ---------- DB ----------
    TABLE = "PaidStorageFlat"
    FIELDS = [
        "org_id",
        "Организация",
        "date",
        "giId",
        "chrtId",
        "logWarehouseCoef",
        "officeId",
        "warehouse",
        "warehouseCoef",
        "size",
        "barcode",
        "subject",
        "brand",
        "vendorCode",
        "nmId",
        "volume",
        "calcType",
        "warehousePrice",
        "barcodesCount",
        "palletPlaceCode",
        "palletCount",
        "originalDate",
        "loyaltyDiscount",
        "tariffFixDate",
        "tariffLowerDate",
        "DateFrom",
        "DateTo",
        "LoadDate",
    ]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        org_id TEXT,
        Организация TEXT,
        date TEXT,
        giId TEXT,
        chrtId TEXT,
        logWarehouseCoef TEXT,
        officeId TEXT,
        warehouse TEXT,
        warehouseCoef TEXT,
        size TEXT,
        barcode TEXT,
        subject TEXT,
        brand TEXT,
        vendorCode TEXT,
        nmId TEXT,
        volume TEXT,
        calcType TEXT,
        warehousePrice TEXT,
        barcodesCount TEXT,
        palletPlaceCode TEXT,
        palletCount TEXT,
        originalDate TEXT,
        loyaltyDiscount TEXT,
        tariffFixDate TEXT,
        tariffLowerDate TEXT,
        DateFrom TEXT,
        DateTo TEXT,
        LoadDate TEXT,
        PRIMARY KEY (org_id, date, giId, chrtId)
    );
    """
    )
    conn.commit()

    def get_org_start_date(org_id: str):
        # Всегда берем только за последнюю неделю
        today = datetime.now().date()
        start_date = today - timedelta(days=7)
        return start_date

    # ---------- WB API ----------
    BASE = "https://seller-analytics-api.wildberries.ru"
    URL_CREATE = f"{BASE}/api/v1/paid_storage"
    URL_STATUS = f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/status"
    URL_DOWNLOAD = f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/download"
    HEADERS_BASE = {"Content-Type": "application/json"}

    def create_task(token: str, dfrom: str, dto: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        return requests.get(
            URL_CREATE, headers=headers, params={"dateFrom": dfrom, "dateTo": dto}, timeout=60
        )

    def get_status(token: str, task_id: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        return requests.get(URL_STATUS.format(task_id=task_id), headers=headers, timeout=60)

    def download_report(token: str, task_id: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        return requests.get(URL_DOWNLOAD.format(task_id=task_id), headers=headers, timeout=120)

    # ---------- Run ----------
    total_inserted = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().date()

    for _, org in df_orgs.iterrows():
        org_id = str(org["id"])
        org_name = str(org["Организация"])
        token = str(org["Token_WB"]).strip()

        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        start_d = get_org_start_date(org_id)
        end_d = today

        if start_d > end_d:
            logger.info(
                "  Нечего догружать (макс.дата уже %s). Пропуск.", start_d - timedelta(days=1)
            )
            continue

        logger.info("  Инкремент: %s .. %s", start_d, end_d)

        for win_from, win_to in daterange_8d(start_d, end_d):
            df_s = iso_date(win_from)
            dt_s = iso_date(win_to)
            logger.info("  окно: %s .. %s", df_s, dt_s)

            # 1) Создаём задание (лимит 1/мин, всплеск 5)
            try:
                r = create_task(token, df_s, dt_s)
                if r.status_code == 429:
                    logger.warning("   429 на create. Жду 65 сек и повторю…")
                    sleep_log(65)
                    r = create_task(token, df_s, dt_s)
                if r.status_code == 401:
                    logger.error("   401 Unauthorized. Пропускаю всю организацию.")
                    break
                if r.status_code != 200:
                    logger.warning("   Ошибка create: %s %s", r.status_code, r.text[:200])
                    sleep_log(2)
                    continue
                task_id = r.json().get("data", {}).get("taskId")
                if not task_id:
                    logger.warning("   taskId не получен.")
                    continue
                logger.info("   taskId: %s", task_id)
            except Exception as e:
                logger.warning("   Ошибка create_task: %s", e)
                continue

            # 2) Ждём статус done (лимит 1/5сек)
            tries, status = 0, ""
            while True:
                tries += 1
                try:
                    st = get_status(token, task_id)
                    if st.status_code == 429:
                        logger.warning("    429 на status. Жду 6 сек…")
                        sleep_log(6)
                        continue
                    if st.status_code != 200:
                        logger.warning("    статус %s: %s", st.status_code, st.text[:200])
                        sleep_log(6)
                        continue
                    status = st.json().get("data", {}).get("status", "")
                    logger.info("    статус: %s", status)
                    if status == "done":
                        break
                    if status in ("error", "failed"):
                        logger.error("    статус ошибки. Пропускаю окно.")
                        break
                except Exception as e:
                    logger.warning("    Ошибка get_status: %s", e)
                sleep_log(5)
                if tries > 60:
                    logger.warning("    слишком долго. Пропускаю окно.")
                    break

            if status != "done":
                continue

            # 3) Скачиваем
            try:
                dw = download_report(token, task_id)
                if dw.status_code == 429:
                    logger.warning("   429 на download. Жду 65 сек и повторю…")
                    sleep_log(65)
                    dw = download_report(token, task_id)
                if dw.status_code != 200:
                    logger.warning("   download: %s %s", dw.status_code, dw.text[:200])
                    continue

                payload = dw.json()
                if not isinstance(payload, list):
                    logger.warning("   Неожиданный формат download (ожидали массив).")
                    continue

                rows = []
                for rec in payload:
                    rows.append(
                        [
                            org_id,
                            org_name,
                            str(rec.get("date", "")),
                            str(rec.get("giId", "")),
                            str(rec.get("chrtId", "")),
                            str(rec.get("logWarehouseCoef", "")),
                            str(rec.get("officeId", "")),
                            str(rec.get("warehouse", "")),
                            str(rec.get("warehouseCoef", "")),
                            str(rec.get("size", "")),
                            str(rec.get("barcode", "")),
                            str(rec.get("subject", "")),
                            str(rec.get("brand", "")),
                            str(rec.get("vendorCode", "")),
                            str(rec.get("nmId", "")),
                            str(rec.get("volume", "")),
                            str(rec.get("calcType", "")),
                            str(rec.get("warehousePrice", "")),
                            str(rec.get("barcodesCount", "")),
                            str(rec.get("palletPlaceCode", "")),
                            str(rec.get("palletCount", "")),
                            str(rec.get("originalDate", "")),
                            str(rec.get("loyaltyDiscount", "")),
                            str(rec.get("tariffFixDate", "")),
                            str(rec.get("tariffLowerDate", "")),
                            df_s,
                            dt_s,
                            now_str,
                        ]
                    )

                if rows:
                    ph = ",".join(["?"] * len(FIELDS))
                    cur.executemany(f"INSERT OR REPLACE INTO {TABLE} VALUES ({ph})", rows)
                    conn.commit()
                    total_inserted += len(rows)
                    logger.info("   ✅ +%s строк (итого: %s)", len(rows), total_inserted)
                else:
                    logger.warning("   Пустой отчёт по этому окну.")

            except Exception as e:
                logger.warning("   Ошибка download/insert: %s", e)
                continue

    logger.info("✅ Готово. Всего вставлено/обновлено строк: %s в %s", total_inserted, TABLE)
    conn.close()


if __name__ == "__main__":
    main()
