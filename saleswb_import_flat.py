from pathlib import Path
import sqlite3
import requests
import json
import time
import pandas as pd

from utils.settings import find_setting, parse_date

# Максимальный размер страницы, заявленный в документации WB API
PAGE_LIMIT = 100_000

# --- Пути ---
base_dir = Path(__file__).resolve().parent.parent
db_path = base_dir / "finmodel.db"
xls_path = base_dir / "Finmodel.xlsm"

# --- Получаем период загрузки из Excel ---
period_start = parse_date(find_setting("ПериодНачало")).strftime("%Y-%m-%dT%H:%M:%S")
period_end   = parse_date(find_setting("ПериодКонец")).strftime("%Y-%m-%dT%H:%M:%S")
print(f"Период загрузки продаж: {period_start} .. {period_end}")

# --- Чтение организаций ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()

# --- Все возможные поля продажи (WB-API) ---
SALES_FIELDS = [
    "date", "lastChangeDate", "warehouseName", "warehouseType", "countryName", "oblastOkrugName", "regionName",
    "supplierArticle", "nmId", "barcode", "category", "subject", "brand", "techSize", "incomeID", "isSupply",
    "isRealization", "totalPrice", "discountPercent", "spp", "paymentSaleAmount", "forPay", "finishedPrice",
    "priceWithDisc", "saleID", "sticker", "gNumber", "srid"
]

# --- Подключение к базе и создание плоской таблицы ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
fields_sql = ", ".join([f"{f} TEXT" for f in SALES_FIELDS])
cursor.execute("DROP TABLE IF EXISTS SalesWBFlat;")
cursor.execute(f"""
CREATE TABLE SalesWBFlat (
    org_id INTEGER,
    Организация TEXT,
    {fields_sql},
    PRIMARY KEY (org_id, srid)
);
""")
conn.commit()

# --- API запрос ---
url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
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
            print("✅ Все продажи загружены для этой организации.")
            break

        # Распаковка
        rows = []
        for rec in data:
            flat = [org_id, org_name] + [str(rec.get(f, "")) for f in SALES_FIELDS]
            rows.append(flat)
        try:
            placeholders = ",".join(["?"] * (2 + len(SALES_FIELDS)))
            cursor.executemany(f"""
                INSERT OR REPLACE INTO SalesWBFlat
                VALUES ({placeholders})
            """, rows)
            conn.commit()
        except Exception as e:
            print(f"\n  ⚠️ Ошибка вставки: {e}")
            break

        total_loaded += len(rows)
        print(f"  +{len(rows)} продаж (итого: {total_loaded})")

        if len(rows) < PAGE_LIMIT:
            print("  ✅ Продажи по периоду загружены полностью.")
            break

        # pagination: следующий dateFrom = lastChangeDate последней строки
        date_from = data[-1].get("lastChangeDate")
        page += 1
        time.sleep(3)  # WB лимит: 1 запрос в минуту, но можно чуть чаще

conn.close()
print("\n✅ Все продажи загружены и распарсены в таблицу SalesWBFlat (без дублей).")
