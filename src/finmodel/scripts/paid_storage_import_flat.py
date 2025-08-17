import sqlite3
import time
from datetime import datetime, timedelta

import requests

from finmodel.logger import get_logger
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations, load_period, parse_date

logger = get_logger(__name__)


def main() -> None:
    # ---------------- Paths ----------------
    db_path = get_db_path()

    logger.info("DB: %s", db_path)

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    def daterange_8d(d1: datetime, d2: datetime):
        """Yield (from, to) windows of up to 8 days inclusive."""
        cur = d1
        one_day = timedelta(days=1)
        while cur <= d2:
            end = min(cur + timedelta(days=7), d2)
            yield cur, end
            cur = end + one_day

    def iso_date(d: datetime) -> str:
        return d.strftime("%Y-%m-%d")

    def sleep_with_log(sec: float, msg: str = ""):
        if msg:
            logger.info(msg)
        time.sleep(sec)

    # ---------------- Load settings ----------------
    period_start_raw, period_end_raw = load_period(sheet=settings_sheet)

    if not period_start_raw or not period_end_raw:
        logger.error("Settings do not include ПериодНачало/ПериодКонец.")
        raise SystemExit(1)

    period_start = parse_date(period_start_raw).date()
    period_end = parse_date(period_end_raw).date()
    if period_end < period_start:
        logger.error("ПериодКонец раньше ПериодНачало.")
        raise SystemExit(1)

    logger.info("Период: %s .. %s", period_start, period_end)

    # Organizations with tokens
    df_orgs = load_organizations(sheet=org_sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # ---------------- DB: table ----------------
    TABLE = "PaidStorageFlat"
    FIELDS = [
        # ключевые
        "org_id",
        "Организация",
        "date",
        "giId",
        "chrtId",
        # прочие из ответа
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
        # сервисные
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

    # ---------------- WB API ----------------
    BASE = "https://seller-analytics-api.wildberries.ru"
    URL_CREATE = f"{BASE}/api/v1/paid_storage"
    URL_STATUS = f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/status"
    URL_DOWNLOAD = f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/download"

    HEADERS_BASE = {"Content-Type": "application/json"}

    def create_task(token: str, dfrom: str, dto: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        params = {"dateFrom": dfrom, "dateTo": dto}
        r = requests.get(URL_CREATE, headers=headers, params=params, timeout=60)
        return r

    def get_status(token: str, task_id: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        r = requests.get(URL_STATUS.format(task_id=task_id), headers=headers, timeout=60)
        return r

    def download_report(token: str, task_id: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        r = requests.get(URL_DOWNLOAD.format(task_id=task_id), headers=headers, timeout=120)
        return r

    # ---------------- Main loop ----------------
    total_inserted = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, org in df_orgs.iterrows():
        org_id = str(org["id"])
        org_name = str(org["Организация"])
        token = str(org["Token_WB"]).strip()

        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        # окна по 8 дней
        for win_from, win_to in daterange_8d(
            datetime.combine(period_start, datetime.min.time()),
            datetime.combine(period_end, datetime.min.time()),
        ):
            df_s = iso_date(win_from)
            dt_s = iso_date(win_to)
            logger.info("  окно: %s .. %s", df_s, dt_s)

            # 1) Create task (учитываем лимит: 1 запрос/мин, всплеск 5)
            try:
                resp = create_task(token, df_s, dt_s)
                if resp.status_code == 429:
                    logger.warning("  429 Too Many Requests на создание. Жду 65 сек и повторю…")
                    sleep_with_log(65)
                    resp = create_task(token, df_s, dt_s)

                if resp.status_code == 401:
                    logger.error("  401 Unauthorized. Пропускаю эту организацию.")
                    break

                if resp.status_code != 200:
                    logger.warning(
                        "  Ошибка создания задания: %s %s", resp.status_code, resp.text[:200]
                    )
                    # мягко продолжаем к следующему окну
                    sleep_with_log(2)
                    continue

                task_id = resp.json().get("data", {}).get("taskId")
                if not task_id:
                    logger.warning("  taskId не получен.")
                    continue
                logger.info("  taskId: %s", task_id)
            except Exception as e:
                logger.warning("  Ошибка create_task: %s", e)
                continue

            # 2) Poll status (лимит: 1 запрос/5 сек)
            status = "queued"
            tries = 0
            while True:
                tries += 1
                try:
                    st = get_status(token, task_id)
                    if st.status_code == 429:
                        logger.warning("   429 на статусе. Жду 6 сек…")
                        sleep_with_log(6)
                        continue
                    if st.status_code != 200:
                        logger.warning("   статус HTTP %s: %s", st.status_code, st.text[:200])
                        sleep_with_log(6)
                        continue
                    status = st.json().get("data", {}).get("status", "")
                    logger.info("   статус: %s", status)
                    if status == "done":
                        break
                    if status in ("error", "failed"):
                        logger.error("   статус ошибки. Пропускаю окно.")
                        break
                except Exception as e:
                    logger.warning("   Ошибка get_status: %s", e)
                # выдерживаем лимит
                sleep_with_log(5)

                if tries > 60:  # ~5 минут ожидания
                    logger.warning("   слишком долго нет 'done'. Пропускаю окно.")
                    break

            if status != "done":
                continue

            # 3) Download
            try:
                dw = download_report(token, task_id)
                if dw.status_code == 429:
                    logger.warning("  429 на download. Жду 65 сек и повторю…")
                    sleep_with_log(65)
                    dw = download_report(token, task_id)

                if dw.status_code != 200:
                    logger.warning("  Ошибка download: %s %s", dw.status_code, dw.text[:200])
                    continue

                rows_json = dw.json()
                if not isinstance(rows_json, list):
                    logger.warning("  Неожиданный формат download (ожидали массив).")
                    continue

                # расплющить и вставить
                rows = []
                for rec in rows_json:
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
                    logger.info("  ✅ +%s строк (итого: %s)", len(rows), total_inserted)
                else:
                    logger.warning("  Пустой отчёт на этом окне.")

            except Exception as e:
                logger.warning("  Ошибка download/insert: %s", e)
                continue

            # Между create задач старайся делать паузы,
            # но так как мы уже долго ждали статус, отдельная пауза не критична.
            # Если догружаете много окон быстро — поставьте тут sleep(60).

    conn.close()
    logger.info("✅ Готово. Всего вставлено/обновлено строк: %s в %s", total_inserted, TABLE)


if __name__ == "__main__":
    main()
