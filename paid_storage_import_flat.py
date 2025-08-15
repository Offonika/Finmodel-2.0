from pathlib import Path
import sqlite3
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

from utils.settings import find_setting, parse_date

# ---------------- Paths ----------------
base_dir = Path(__file__).resolve().parent.parent
db_path  = base_dir / "finmodel.db"
xls_path = base_dir / "Finmodel.xlsm"

print(f"DB:  {db_path}")
print(f"XLS: {xls_path}")

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
        print(msg)
    time.sleep(sec)

# ---------------- Load settings ----------------
period_start_raw = find_setting("ПериодНачало")
period_end_raw   = find_setting("ПериодКонец")

if not period_start_raw or not period_end_raw:
    print("❗ В листе 'Настройки' не найдены ПериодНачало/ПериодКонец.")
    raise SystemExit(1)

period_start = parse_date(period_start_raw).date()
period_end   = parse_date(period_end_raw).date()
if period_end < period_start:
    print("❗ ПериодКонец раньше ПериодНачало.")
    raise SystemExit(1)

print(f"Период: {period_start} .. {period_end}")

# Orgs with tokens
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()
if df_orgs.empty:
    print("❗ Лист 'НастройкиОрганизаций' пуст или нет нужных колонок.")
    raise SystemExit(1)

# ---------------- DB: table ----------------
TABLE = "PaidStorageFlat"
FIELDS = [
    # ключевые
    "org_id","Организация","date","giId","chrtId",
    # прочие из ответа
    "logWarehouseCoef","officeId","warehouse","warehouseCoef",
    "size","barcode","subject","brand","vendorCode","nmId","volume",
    "calcType","warehousePrice","barcodesCount","palletPlaceCode","palletCount",
    "originalDate","loyaltyDiscount","tariffFixDate","tariffLowerDate",
    # сервисные
    "DateFrom","DateTo","LoadDate"
]

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute(f"""
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
""")
conn.commit()

# ---------------- WB API ----------------
BASE = "https://seller-analytics-api.wildberries.ru"
URL_CREATE  = f"{BASE}/api/v1/paid_storage"
URL_STATUS  = f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/status"
URL_DOWNLOAD= f"{BASE}/api/v1/paid_storage/tasks/{{task_id}}/download"

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
    org_id   = str(org["id"])
    org_name = str(org["Организация"])
    token    = str(org["Token_WB"]).strip()

    print(f"\n→ Организация: {org_name} (ID={org_id})")

    # окна по 8 дней
    for win_from, win_to in daterange_8d(datetime.combine(period_start, datetime.min.time()),
                                         datetime.combine(period_end,   datetime.min.time())):
        df_s = iso_date(win_from)
        dt_s = iso_date(win_to)
        print(f"  окно: {df_s} .. {dt_s}")

        # 1) Create task (учитываем лимит: 1 запрос/мин, всплеск 5)
        try:
            resp = create_task(token, df_s, dt_s)
            if resp.status_code == 429:
                print("  ⚠️ 429 Too Many Requests на создание. Жду 65 сек и повторю…")
                sleep_with_log(65)
                resp = create_task(token, df_s, dt_s)

            if resp.status_code == 401:
                print(f"  ❗ 401 Unauthorized. Пропускаю эту организацию.")
                break

            if resp.status_code != 200:
                print(f"  ⚠️ Ошибка создания задания: {resp.status_code} {resp.text[:200]}")
                # мягко продолжаем к следующему окну
                sleep_with_log(2)
                continue

            task_id = resp.json().get("data", {}).get("taskId")
            if not task_id:
                print("  ⚠️ taskId не получен.")
                continue
            print(f"  taskId: {task_id}")
        except Exception as e:
            print(f"  ⚠️ Ошибка create_task: {e}")
            continue

        # 2) Poll status (лимит: 1 запрос/5 сек)
        status = "queued"
        tries  = 0
        while True:
            tries += 1
            try:
                st = get_status(token, task_id)
                if st.status_code == 429:
                    print("   429 на статусе. Жду 6 сек…")
                    sleep_with_log(6)
                    continue
                if st.status_code != 200:
                    print(f"   ⚠️ статус HTTP {st.status_code}: {st.text[:200]}")
                    sleep_with_log(6)
                    continue
                status = st.json().get("data", {}).get("status", "")
                print(f"   статус: {status}")
                if status == "done":
                    break
                if status in ("error","failed"):
                    print("   ❗ статус ошибки. Пропускаю окно.")
                    break
            except Exception as e:
                print(f"   ⚠️ Ошибка get_status: {e}")
            # выдерживаем лимит
            sleep_with_log(5)

            if tries > 60:  # ~5 минут ожидания
                print("   ⚠️ слишком долго нет 'done'. Пропускаю окно.")
                break

        if status != "done":
            continue

        # 3) Download
        try:
            dw = download_report(token, task_id)
            if dw.status_code == 429:
                print("  429 на download. Жду 65 сек и повторю…")
                sleep_with_log(65)
                dw = download_report(token, task_id)

            if dw.status_code != 200:
                print(f"  ⚠️ Ошибка download: {dw.status_code} {dw.text[:200]}")
                continue

            rows_json = dw.json()
            if not isinstance(rows_json, list):
                print("  ⚠️ Неожиданный формат download (ожидали массив).")
                continue

            # расплющить и вставить
            rows = []
            for rec in rows_json:
                rows.append([
                    org_id, org_name,
                    str(rec.get("date","")),
                    str(rec.get("giId","")),
                    str(rec.get("chrtId","")),
                    str(rec.get("logWarehouseCoef","")),
                    str(rec.get("officeId","")),
                    str(rec.get("warehouse","")),
                    str(rec.get("warehouseCoef","")),
                    str(rec.get("size","")),
                    str(rec.get("barcode","")),
                    str(rec.get("subject","")),
                    str(rec.get("brand","")),
                    str(rec.get("vendorCode","")),
                    str(rec.get("nmId","")),
                    str(rec.get("volume","")),
                    str(rec.get("calcType","")),
                    str(rec.get("warehousePrice","")),
                    str(rec.get("barcodesCount","")),
                    str(rec.get("palletPlaceCode","")),
                    str(rec.get("palletCount","")),
                    str(rec.get("originalDate","")),
                    str(rec.get("loyaltyDiscount","")),
                    str(rec.get("tariffFixDate","")),
                    str(rec.get("tariffLowerDate","")),
                    df_s, dt_s,
                    now_str
                ])

            if rows:
                ph = ",".join(["?"] * len(FIELDS))
                cur.executemany(f"INSERT OR REPLACE INTO {TABLE} VALUES ({ph})", rows)
                conn.commit()
                total_inserted += len(rows)
                print(f"  ✅ +{len(rows)} строк (итого: {total_inserted})")
            else:
                print("  ⚠️ Пустой отчёт на этом окне.")

        except Exception as e:
            print(f"  ⚠️ Ошибка download/insert: {e}")
            continue

        # Между create задач старайся делать паузы,
        # но так как мы уже долго ждали статус, отдельная пауза не критична.
        # Если догружаете много окон быстро — поставьте тут sleep(60).

conn.close()
print(f"\n✅ Готово. Всего вставлено/обновлено строк: {total_inserted} в {TABLE}")
