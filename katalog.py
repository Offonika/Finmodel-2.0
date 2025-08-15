import os
import sqlite3
import requests
import time
import pandas as pd

# 📌 Пути к базе и Excel-файлу
base_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
db_path = os.path.join(base_dir, "finmodel.db")
xls_path = os.path.join(base_dir, "Finmodel.xlsm")

# 📌 Чтение таблицы организаций
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()

# 📌 Подключение к базе
try:
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()
except sqlite3.OperationalError as e:
    print(f"Ошибка подключения к базе: {e}")
    exit(1)

# 📌 Пересоздание таблицы с нужными столбцами
cursor.execute("DROP TABLE IF EXISTS katalog;")
cursor.execute("""
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
""")
conn.commit()

# Показываем структуру таблицы
cursor.execute("PRAGMA table_info(katalog);")
print("СТРУКТУРА katalog:", cursor.fetchall())

# 📌 Wildberries API
url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
headers_template = {"Content-Type": "application/json"}

# 📌 Обработка всех организаций
for _, row in df_orgs.iterrows():
    org_id = row["id"]
    org_name = row["Организация"]
    token = row["Token_WB"]

    print(f"→ Организация: {org_name} (ID={org_id})")
    headers = headers_template.copy()
    headers["Authorization"] = token

    has_more = True
    updatedAt = None
    nmID = None

    while has_more:
        payload = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1}
            }
        }

        if updatedAt and nmID:
            payload["settings"]["cursor"].update({
                "updatedAt": updatedAt,
                "nmID": nmID
            })

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            data = response.json()
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            break

        cards = data.get("cards", [])
        if not cards:
            print("  Нет карточек.")
            break

        rows = []
        for card in cards:
            createdAt = card.get("createdAt")
            updatedAtCard = card.get("updatedAt")
            for size in card.get("sizes", []):
                techSize = size.get("techSize")
                chrtID = size.get("chrtID")
                for sku in size.get("skus", []):
                    rows.append((
                        org_id,
                        org_name,
                        card.get("nmID"),
                        card.get("imtID"),
                        card.get("nmUUID"),
                        card.get("subjectID"),
                        card.get("subjectName"),
                        card.get("brand"),
                        card.get("vendorCode"),
                        techSize,
                        sku,
                        chrtID,
                        createdAt,
                        updatedAtCard
                    ))

        if rows:
            try:
                cursor.executemany("""
                    REPLACE INTO katalog (
                        org_id, Организация, nmID, imtID, nmUUID,
                        subjectID, subjectName, brand, vendorCode,
                        techSize, sku, chrtID, createdAt, updatedAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                conn.commit()
            except Exception as e:
                print(f"Ошибка записи в БД: {e}")
                break

        # Обновление курсора
        cursor_data = data.get("cursor", {})
        updatedAt = cursor_data.get("updatedAt")
        nmID = cursor_data.get("nmID")
        total = cursor_data.get("total", 0)

        print(f"  Загружено {len(cards)} карточек, осталось ~{total}")
        has_more = total >= 100
        if has_more:
            time.sleep(0.6)

# 📌 Завершение
conn.close()
print("✅ Все карточки успешно загружены в таблицу katalog (без дублей).")
