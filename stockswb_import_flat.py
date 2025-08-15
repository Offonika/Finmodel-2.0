from pathlib import Path
import sqlite3
import requests
import json
import time
import pandas as pd
from datetime import datetime

# --- Пути ---
base_dir = Path(__file__).resolve().parent.parent
db_path = base_dir / "finmodel.db"
xls_path = base_dir / "Finmodel.xlsm"

# --- Получаем "ПериодНачало" из Excel ---
df_settings = pd.read_excel(xls_path, sheet_name="Настройки", engine="openpyxl")
def find_setting(name):
    val = df_settings.loc[df_settings["Параметр"].str.strip() == name, "Значение"]
    return val.values[0] if not val.empty else None
def parse_date(dt):
    s = str(dt).replace("T", " ").replace("/", ".").strip()
    try:
        return datetime.strptime(s, "%d.%m.%Y").strftime("%Y-%m-%dT00:00:00")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00")
        except Exception:
            return pd.to_datetime(s).strftime("%Y-%m-%dT%H:%M:%S")
period_start = parse_date(find_setting("ПериодНачало"))
print(f"Дата начала загрузки остатков: {period_start}")

# --- Чтение организаций ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()

# --- Все возможные поля остатков (WB-API) ---
STOCKS_FIELDS = [
    "lastChangeDate", "warehouseName", "supplierArticle", "nmId", "barcode", "quantity", "inWayToClient",
    "inWayFromClient", "quantityFull", "category", "subject", "brand", "techSize", "Price", "Discount",
    "isSupply", "isRealization", "SCCode"
]

# --- Пересоздание таблицы ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
fields_sql = ", ".join([f"{f} TEXT" for f in STOCKS_FIELDS])
cursor.execute("DROP TABLE IF EXISTS StocksWBFlat;")
cursor.execute(f"""
CREATE TABLE StocksWBFlat (
    org_id INTEGER,
    Организация TEXT,
    {fields_sql},
    PRIMARY KEY (org_id, nmId, warehouseName)
);
""")
conn.commit()

# --- API-запрос ---
url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
headers_template = {"Content-Type": "application/json"}

for _, row in df_orgs.iterrows():
    org_id = row["id"]
    org_name = row["Организация"]
    token = row["Token_WB"]
    print(f"\n→ Организация: {org_name} (ID={org_id})")

    headers = headers_template.copy()
    headers["Authorization"] = token

    date_from = period_start
    total_loaded = 0
    page = 1

    while True:
        params = {"dateFrom": date_from}
        print(f"  📤 Запрос page {page}, dateFrom={date_from} ...", end=" ", flush=True)
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60)
            if resp.status_code != 200:
                print(f"\n  ⚠️ Запрос вернул статус {resp.status_code}: {resp.text}")
                time.sleep(5)
                break
            data = resp.json()
        except Exception as e:
            print(f"\n  ⚠️ Ошибка запроса: {e}")
            time.sleep(5)
            break

        if not data:
            print("✅ Все остатки загружены для этой организации.")
            break

        # Распаковка
        rows = []
        for rec in data:
            flat = [org_id, org_name] + [str(rec.get(f, "")) for f in STOCKS_FIELDS]
            rows.append(flat)
        try:
            placeholders = ",".join(["?"] * (2 + len(STOCKS_FIELDS)))
            cursor.executemany(f"""
                INSERT OR REPLACE INTO StocksWBFlat
                VALUES ({placeholders})
            """, rows)
            conn.commit()
        except Exception as e:
            print(f"\n  ⚠️ Ошибка вставки: {e}")
            break

        total_loaded += len(rows)
        print(f"  +{len(rows)} остатков (итого: {total_loaded})")

        if len(rows) == 0 or len(rows) < 60000:
            print("  ✅ Остатки по периоду загружены полностью.")
            break

        # pagination: следующий dateFrom = lastChangeDate последней строки
        date_from = data[-1].get("lastChangeDate")
        page += 1
        time.sleep(3)  # WB лимит: 1 запрос в минуту, но можно чуть чаще

conn.close()
print("\n✅ Все остатки загружены и распарсены в таблицу StocksWBFlat (без дублей).")
