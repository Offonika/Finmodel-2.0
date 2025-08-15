from pathlib import Path
import sqlite3
import requests
import pandas as pd
import time
from datetime import datetime

# --- Пути ---
base_dir = Path(__file__).resolve().parent.parent
db_path  = base_dir / "finmodel.db"
xls_path = base_dir / "Finmodel.xlsm"

print(f"DB: {db_path}")
print(f"XLSX: {xls_path}")

# --- Читаем организации/токены ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()

# --- Итоговые поля таблицы ---
FIELDS = [
    "org_id","Организация",
    "campaignId","campaignName",
    "campaignType","campaignStatus",
    "lastChangeDate","LoadDate",
]

# --- Пересоздаём таблицу ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS AdvCampaignsFlat;")
cursor.execute(f"""
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
""")
conn.commit()

URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
HEADERS_BASE = {"Content-Type": "application/json"}

def norm_ts(v):
    if not v or str(v).strip()=="":
        return ""
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(v)

total_rows = 0

for _, r in df_orgs.iterrows():
    org_id   = str(r["id"])
    org_name = str(r["Организация"])
    token    = str(r["Token_WB"]).strip()

    print(f"\n→ Организация: {org_name} (ID={org_id})")
    headers = HEADERS_BASE.copy()
    headers["Authorization"] = token

    try:
        resp = requests.get(URL, headers=headers, timeout=60)
        print(f"  HTTP {resp.status_code}")
        preview = (resp.text or "")[:500].replace("\n"," ")
        print("  Ответ (начало):", preview if preview else "[пусто]")

        if resp.status_code != 200:
            continue

        data = resp.json() or {}
    except Exception as e:
        print(f"  ⚠️ Ошибка запроса: {e}")
        time.sleep(0.3)
        continue

    adverts = data.get("adverts", [])
    if not isinstance(adverts, list) or not adverts:
        print("  ⚠️ Пустой список adverts.")
        time.sleep(0.3)
        continue

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    # ожидаемый формат: adverts: [{type, status, count, advert_list: [{advertId, changeTime}, ...]}, ...]
    for group in adverts:
        camp_type   = str(group.get("type", ""))     # числовой код типа
        camp_status = str(group.get("status", ""))   # числовой код статуса
        items       = group.get("advert_list", []) or []
        for it in items:
            advert_id = it.get("advertId")
            change_ts = norm_ts(it.get("changeTime"))
            if advert_id is None:
                continue
            rows.append([
                org_id, org_name,
                str(advert_id),
                "",                 # имя кампании из этого метода не приходит
                camp_type,
                camp_status,
                change_ts,
                now_str
            ])

    if not rows:
        print("  ⚠️ Кампаний не найдено по этому токену.")
        time.sleep(0.3)
        continue

    try:
        placeholders = ",".join(["?"] * len(FIELDS))
        cursor.executemany(f"INSERT OR REPLACE INTO AdvCampaignsFlat VALUES ({placeholders})", rows)
        conn.commit()
        total_rows += len(rows)
        print(f"  ✅ Загружено {len(rows)} кампаний (плоско).")
    except Exception as e:
        print(f"  ⚠️ Ошибка вставки: {e}")

    time.sleep(0.3)  # лимит 5 req/sec

conn.close()
print(f"\n✅ Готово. Всего записей добавлено/обновлено: {total_rows}")
