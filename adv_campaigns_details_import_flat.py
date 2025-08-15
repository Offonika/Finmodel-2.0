import os
import sqlite3
import requests
import pandas as pd
import time
from datetime import datetime

# --- Пути ---
base_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))
db_path  = os.path.join(base_dir, "finmodel.db")
xls_path = os.path.join(base_dir, "Finmodel.xlsm")

print(f"DB: {db_path}")
print(f"XLSX: {xls_path}")

# --- Читаем организации/токены ---
df_orgs = pd.read_excel(xls_path, sheet_name="НастройкиОрганизаций", engine="openpyxl")
df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()
if df_orgs.empty:
    print("❗ Лист 'НастройкиОрганизаций' пуст или нет колонок id/Организация/Token_WB")
    raise SystemExit(1)

# --- Итоговая плоская таблица (пересоздаём на каждый запуск) ---
TABLE_NAME = "AdvCampaignsDetailsFlat"
FIELDS = [
    "org_id","Организация",
    "advertId","name","status","type","paymentType",
    "startTime","endTime","createTime","changeTime",
    "dailyBudget","searchPluseState",
    "param_index","interval_begin","interval_end","price",
    "subjectId","subjectName","param_active",
    "nm","nm_active",
    "LoadDate"
]

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
cursor.execute(f"""
CREATE TABLE {TABLE_NAME} (
  org_id TEXT,
  Организация TEXT,
  advertId TEXT,
  name TEXT,
  status TEXT,
  type TEXT,
  paymentType TEXT,
  startTime TEXT,
  endTime TEXT,
  createTime TEXT,
  changeTime TEXT,
  dailyBudget TEXT,
  searchPluseState TEXT,
  param_index TEXT,
  interval_begin TEXT,
  interval_end TEXT,
  price TEXT,
  subjectId TEXT,
  subjectName TEXT,
  param_active TEXT,
  nm TEXT,
  nm_active TEXT,
  LoadDate TEXT,
  PRIMARY KEY (org_id, advertId, param_index, nm)
);
""")
conn.commit()

# --- Эндпоинты и базовые заголовки ---
URL_COUNT   = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
URL_DETAILS = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
HEADERS_BASE = {"Content-Type": "application/json"}

# --- Фильтры: нужны status ∈ {9, 11} и type ∈ {8, 9} ---
ALLOWED_STATUS = {"9", "11"}   # 9 - активно, 11 - пауза
ALLOWED_TYPE   = {"8", "9"}    # 8 - автоматическая, 9 - аукцион

def norm_ts(v):
    if not v or str(v).strip()=="":
        return ""
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(v)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

total_rows = 0
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for _, r in df_orgs.iterrows():
    org_id   = str(r["id"])
    org_name = str(r["Организация"])
    token    = str(r["Token_WB"]).strip()

    print(f"\n→ Организация: {org_name} (ID={org_id})")
    headers = HEADERS_BASE.copy()
    headers["Authorization"] = token

    # 1) Получаем ID кампаний через /promotion/count
    try:
        resp = requests.get(URL_COUNT, headers=headers, timeout=60)
        print(f"  [count] HTTP {resp.status_code}")
        if resp.status_code != 200:
            print(f"  ⚠️ Ошибка count: {resp.text[:300]}")
            time.sleep(0.3)
            continue
        data = resp.json() or {}
    except Exception as e:
        print(f"  ⚠️ Ошибка запроса count: {e}")
        time.sleep(0.3)
        continue

    # Ожидаем формат: {"adverts":[{"type":..,"status":..,"count":..,"advert_list":[{"advertId":..,"changeTime":..}, ...]}], "all": N}
    adverts = data.get("adverts", [])
    if not isinstance(adverts, list) or not adverts:
        print("  ⚠️ Нет кампаний в ответе count.")
        time.sleep(0.3)
        continue

    # Фильтруем по статусу и типу, собираем ID
    advert_ids = []
    for grp in adverts:
        t = str(grp.get("type",""))
        s = str(grp.get("status",""))
        if t in ALLOWED_TYPE and s in ALLOWED_STATUS:
            for it in grp.get("advert_list", []) or []:
                if "advertId" in it and it["advertId"] is not None:
                    advert_ids.append(str(it["advertId"]))

    advert_ids = sorted(set(advert_ids))
    if not advert_ids:
        print("  ⚠️ После фильтра status∈{9,11}, type∈{8,9} кампаний нет.")
        time.sleep(0.3)
        continue

    print(f"  ▶ Отобрано кампаний по фильтру: {len(advert_ids)} (будем запрашивать подробнее)")

    # 2) Получаем детали кампаниями партиями по 50 ID
    rows_to_insert = []

    for batch in chunks(advert_ids, 50):
        try:
            # можно указать порядок: например, по последнему изменению
            params = {"order": "change", "direction": "desc"}
            resp = requests.post(URL_DETAILS, headers=headers, params=params, json=[int(x) for x in batch], timeout=60)
            # соблюдаем лимиты 5 req/s
            time.sleep(0.3)

            print(f"  [adverts] ids={len(batch)} → HTTP {resp.status_code}")
            if resp.status_code != 200:
                print(f"  ⚠️ Ошибка adverts: {resp.text[:300]}")
                continue

            details = resp.json() or []
            if not isinstance(details, list):
                print("  ⚠️ Неожиданный формат adverts (ожидали массив). Пропуск батча.")
                continue

            # Разворачиваем params/intervals/nms
            for camp in details:
                advertId         = str(camp.get("advertId",""))
                name             = str(camp.get("name",""))
                status           = str(camp.get("status",""))
                ctype            = str(camp.get("type",""))
                paymentType      = str(camp.get("paymentType",""))
                startTime        = norm_ts(camp.get("startTime",""))
                endTime          = norm_ts(camp.get("endTime",""))
                createTime       = norm_ts(camp.get("createTime",""))
                changeTime       = norm_ts(camp.get("changeTime",""))
                dailyBudget      = str(camp.get("dailyBudget",""))
                searchPluseState = str(camp.get("searchPluseState",""))

                params_list = camp.get("params", []) or [None]  # если нет params — дадим одну «пустую» итерацию
                for p_idx, p in enumerate(params_list):
                    if not p:
                        # пустые params → одна строка без доп.разворачивания
                        rows_to_insert.append([
                            org_id, org_name,
                            advertId, name, status, ctype, paymentType,
                            startTime, endTime, createTime, changeTime,
                            dailyBudget, searchPluseState,
                            str(p_idx), "", "", "", "", "", "",
                            "", "",  # nm, nm_active
                            now_str
                        ])
                        continue

                    intervals = p.get("intervals", []) or [None]
                    nms      = p.get("nms", []) or [None]

                    # Если нужно «все комбинации» интервалов и nm:
                    for interval in intervals:
                        begin = str(interval.get("begin","")) if interval else ""
                        end   = str(interval.get("end","")) if interval else ""

                        for nm_item in nms:
                            nm_val     = str(nm_item.get("nm","")) if nm_item else ""
                            nm_active  = str(nm_item.get("active","")) if nm_item else ""

                            rows_to_insert.append([
                                org_id, org_name,
                                advertId, name, status, ctype, paymentType,
                                startTime, endTime, createTime, changeTime,
                                dailyBudget, searchPluseState,
                                str(p_idx),
                                begin, end,
                                str(p.get("price","")),
                                str(p.get("subjectId","")),
                                str(p.get("subjectName","")),
                                str(p.get("active","")),
                                nm_val, nm_active,
                                now_str
                            ])

        except Exception as e:
            print(f"  ⚠️ Ошибка запроса adverts: {e}")

    if not rows_to_insert:
        print("  ⚠️ Деталей кампаний не получено (после adverts).")
        continue

    try:
        placeholders = ",".join(["?"] * len(FIELDS))
        cursor.executemany(f"INSERT OR REPLACE INTO {TABLE_NAME} VALUES ({placeholders})", rows_to_insert)
        conn.commit()
        total_rows += len(rows_to_insert)
        print(f"  ✅ Вставлено {len(rows_to_insert)} строк в {TABLE_NAME}")
    except Exception as e:
        print(f"  ⚠️ Ошибка вставки: {e}")

conn.close()
print(f"\n✅ Готово. Всего добавлено/обновлено строк: {total_rows} в {TABLE_NAME}")
