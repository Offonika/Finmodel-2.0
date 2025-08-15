import os
import sqlite3
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# --- Пути ---
base_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
db_path  = os.path.join(base_dir, "finmodel.db")
xls_path = os.path.join(base_dir, "Finmodel.xlsm")

print(f"DB:  {db_path}")
print(f"XLS: {xls_path}")

# --- Период: всегда последние 7 дней (включая сегодня) ---
today = datetime.now().date()
date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
date_to   = today.strftime("%Y-%m-%d")
print(f"Период запроса: {date_from} .. {date_to}")

# --- Чтение организаций и токенов ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()
if df_orgs.empty:
    print("❗ Нет организаций/токенов в листе 'НастройкиОрганизаций'.")
    raise SystemExit(1)

# --- Подключение к БД ---
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# --- Таблица результата (плоская) ---
TABLE = "WB_NMReportHistory"
cur.execute(f"""
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
""")
conn.commit()

# --- Хелперы ---
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
HEADERS_BASE = {"Content-Type": "application/json"}
SLEEP_BETWEEN_CALLS = 20  # лимит 3 req/min → держим 20 сек

def get_nmids_for_org(c, org_id, org_name):
    # 1) katalog
    try:
        rows = c.execute("SELECT DISTINCT nmID FROM katalog WHERE org_id = ? AND nmID IS NOT NULL", (org_id,)).fetchall()
        nmids = [int(r[0]) for r in rows if str(r[0]).strip() != ""]
        if nmids:
            return sorted(set(nmids))
    except Exception:
        pass
    # 2) WBGoodsPricesFlat
    try:
        rows = c.execute("SELECT DISTINCT nmID FROM WBGoodsPricesFlat WHERE org_id = ? AND nmID IS NOT NULL", (org_id,)).fetchall()
        nmids = [int(r[0]) for r in rows if str(r[0]).strip() != ""]
        if nmids:
            return sorted(set(nmids))
    except Exception:
        pass
    print(f"  ⚠️ Не найдены nmID для организации {org_name} (ID={org_id}).")
    return []

def do_request(token, nm_ids):
    headers = HEADERS_BASE.copy()
    headers["Authorization"] = token
    body = {
        "nmIDs": nm_ids,  # максимум 20
        "period": {"begin": date_from, "end": date_to},
        "timezone": "Europe/Moscow",
        "aggregationLevel": "day"
    }
    resp = requests.post(API_URL, headers=headers, json=body, timeout=90)
    return resp

# --- Основной цикл по организациям ---
total_inserted = 0
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for _, org in df_orgs.iterrows():
    org_id   = str(org["id"])
    org_name = str(org["Организация"])
    token    = str(org["Token_WB"]).strip()

    print(f"\n→ Организация: {org_name} (ID={org_id})")
    nmids = get_nmids_for_org(cur, org_id, org_name)
    if not nmids:
        continue

    print(f"  Всего nmID: {len(nmids)} (батчи по 20)")

    batch_num = 0
    for batch in chunked(nmids, 20):
        batch_num += 1
        print(f"  ▶ Батч {batch_num}: {len(batch)} nmID", flush=True)

        try:
            resp = do_request(token, batch)
            # Если упёрлись в лимит — немного подождём и повторим 1 раз
            if resp.status_code == 429:
                print("    429 Too Many Requests. Жду 25 сек и повторяю…")
                time.sleep(25)
                resp = do_request(token, batch)

            if resp.status_code == 401:
                print("    ❗ 401 Unauthorized — проверьте токен/права (нужна аналитика). Пропускаю организацию.")
                break

            if resp.status_code != 200:
                print(f"    ⚠️ HTTP {resp.status_code}: {resp.text[:300]}")
                # даже при ошибке соблюдём паузу между вызовами
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            payload = resp.json() or {}
            data = payload.get("data", [])
            if not isinstance(data, list):
                print("    ⚠️ Неожиданный формат ответа (ожидали массив 'data').")
                time.sleep(SLEEP_BETWEEN_CALLS)
                continue

            rows = []
            for item in data:
                nm = str(item.get("nmID",""))
                imtName = str(item.get("imtName",""))
                vendorCode = str(item.get("vendorCode",""))
                history = item.get("history", []) or []
                for h in history:
                    rows.append([
                        org_id, org_name,
                        nm, imtName, vendorCode,
                        str(h.get("dt","")),
                        str(h.get("openCardCount","")),
                        str(h.get("addToCartCount","")),
                        str(h.get("ordersCount","")),
                        str(h.get("ordersSumRub","")),
                        str(h.get("buyoutsCount","")),
                        str(h.get("buyoutsSumRub","")),
                        str(h.get("buyoutPercent","")),
                        str(h.get("addToCartConversion","")),
                        str(h.get("cartToOrderConversion","")),
                        now_str
                    ])

            if rows:
                ph = ",".join(["?"] * 16)
                cur.executemany(f"INSERT OR REPLACE INTO {TABLE} VALUES ({ph})", rows)
                conn.commit()
                total_inserted += len(rows)
                print(f"    ✅ +{len(rows)} строк (итого: {total_inserted})")
            else:
                print("    ⚠️ Пустые данные по этому батчу.")

        except Exception as e:
            print(f"    ⚠️ Ошибка запроса/вставки: {e}")

        # строго выдерживаем лимит 3 запроса/мин → пауза 20 секунд
        time.sleep(SLEEP_BETWEEN_CALLS)

conn.close()
print(f"\n✅ Готово. Всего добавлено/обновлено строк: {total_inserted} в {TABLE}")
