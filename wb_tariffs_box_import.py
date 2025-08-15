import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime

# --- Пути ---
base_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
db_path  = os.path.join(base_dir, "finmodel.db")
xls_path = os.path.join(base_dir, "Finmodel.xlsm")

print(f"DB:  {db_path}")
print(f"XLS: {xls_path}")

# --- Дата запроса: берём из 'Настройки' (ПериодКонец), иначе сегодня ---
def get_date_param():
    try:
        df_set = pd.read_excel(xls_path, sheet_name="Настройки", engine="openpyxl")
        val = df_set.loc[df_set["Параметр"].astype(str).str.strip() == "ПериодКонец", "Значение"]
        if not val.empty:
            s = str(val.values[0]).strip().replace("T", " ")
            try:
                return pd.to_datetime(s).strftime("%Y-%m-%d")
            except Exception:
                pass
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d")

date_param = get_date_param()
print(f"Дата для запроса тарифов: {date_param}")

# --- Чтение токенов (перебор до первого рабочего) ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
tokens = (
    df_orgs["Token_WB"]
    .dropna()
    .astype(str)
    .map(str.strip)
    .tolist()
)

if not tokens:
    print("❗ Не найдено ни одного токена в 'НастройкиОрганизаций'.")
    raise SystemExit(1)

# --- Итоговая таблица (пересоздаём) ---
TABLE = "WBTariffsBox"
FIELDS = [
    "DateParam",                 # дата, по которой запрашивали тарифы
    "dtNextBox", "dtTillMax",
    "warehouseName", "geoName",
    "boxDeliveryAndStorageExpr",
    "boxDeliveryBase", "boxDeliveryCoefExpr", "boxDeliveryLiter",
    "boxDeliveryMarketplaceBase", "boxDeliveryMarketplaceCoefExpr", "boxDeliveryMarketplaceLiter",
    "boxStorageBase", "boxStorageCoefExpr", "boxStorageLiter",
    "LoadDate"
]

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
cur.execute(f"""
CREATE TABLE {TABLE} (
    DateParam TEXT,
    dtNextBox TEXT,
    dtTillMax TEXT,
    warehouseName TEXT,
    geoName TEXT,
    boxDeliveryAndStorageExpr TEXT,
    boxDeliveryBase TEXT,
    boxDeliveryCoefExpr TEXT,
    boxDeliveryLiter TEXT,
    boxDeliveryMarketplaceBase TEXT,
    boxDeliveryMarketplaceCoefExpr TEXT,
    boxDeliveryMarketplaceLiter TEXT,
    boxStorageBase TEXT,
    boxStorageCoefExpr TEXT,
    boxStorageLiter TEXT,
    LoadDate TEXT
);
""")
conn.commit()

URL = "https://common-api.wildberries.ru/api/v1/tariffs/box"
params = {"date": date_param}

def try_fetch(token: str):
    headers = {"Authorization": token}
    try:
        r = requests.get(URL, headers=headers, params=params, timeout=60)
        print(f"  Тест токена → HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"  Ответ: {r.text[:300]}")
            return None
        return r.json()
    except Exception as e:
        print(f"  Ошибка сети: {e}")
        return None

data = None
for i, tk in enumerate(tokens, 1):
    print(f"\nПробую токен №{i} ...")
    data = try_fetch(tk)
    if data:
        print("  ✅ Данные получены.")
        break

if not data:
    print("\n❗ Не удалось получить тарифы ни с одним токеном. Проверьте сеть/доступ/токены.")
    conn.close()
    raise SystemExit(2)

# --- Разворачиваем структуру ---
# Ожидаем: {"response":{"data":{"dtNextBox":"...","dtTillMax":"...","warehouseList":[ {...}, ... ]}}}
resp = data.get("response", {})
dat  = resp.get("data", {})
dtNextBox = str(dat.get("dtNextBox", ""))
dtTillMax = str(dat.get("dtTillMax", ""))
warehouses = dat.get("warehouseList", []) or []

now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
rows = []
for wh in warehouses:
    rows.append([
        date_param,
        dtNextBox, dtTillMax,
        str(wh.get("warehouseName","")),
        str(wh.get("geoName","")),
        str(wh.get("boxDeliveryAndStorageExpr","")),
        str(wh.get("boxDeliveryBase","")),
        str(wh.get("boxDeliveryCoefExpr","")),
        str(wh.get("boxDeliveryLiter","")),
        str(wh.get("boxDeliveryMarketplaceBase","")),
        str(wh.get("boxDeliveryMarketplaceCoefExpr","")),
        str(wh.get("boxDeliveryMarketplaceLiter","")),
        str(wh.get("boxStorageBase","")),
        str(wh.get("boxStorageCoefExpr","")),
        str(wh.get("boxStorageLiter","")),
        now_str
    ])

if not rows:
    print("⚠️ Список складов пуст. Возможно, на эту дату тарификация не определена.")
else:
    placeholders = ",".join(["?"] * len(FIELDS))
    cur.executemany(f"INSERT INTO {TABLE} VALUES ({placeholders})", rows)
    conn.commit()
    print(f"✅ Вставлено строк: {len(rows)} в {TABLE}")

conn.close()
print("\nГотово.")
