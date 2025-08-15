import json
import sqlite3
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from finmodel.utils.settings import find_setting, load_config, parse_date


def main(config=None):
    config = config or load_config()
    # Максимальный размер страницы, заявленный в документации WB API
    PAGE_LIMIT = 100_000
    REQUEST_TIMEOUT = 60

    # --- Пути ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = Path(config.get("db_path", base_dir / "finmodel.db"))

    # --- Получаем период загрузки ---
    period_start = parse_date(find_setting("ПериодНачало")).strftime("%Y-%m-%dT%H:%M:%S")
    period_end = parse_date(find_setting("ПериодКонец")).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Период загрузки заказов: {period_start} .. {period_end}")

    # --- Чтение организаций ---
    df_orgs = pd.DataFrame(config.get("organizations", []))
    df_orgs = df_orgs[["id", "Организация", "Token_WB"]].dropna()
    if df_orgs.empty:
        print("❗ Конфигурация не содержит организаций с токенами.")
        raise SystemExit(1)

    # --- Все возможные поля заказа (WB-API) ---
    ORDER_FIELDS = [
        "date",
        "lastChangeDate",
        "warehouseName",
        "warehouseType",
        "countryName",
        "oblastOkrugName",
        "regionName",
        "supplierArticle",
        "nmId",
        "barcode",
        "category",
        "subject",
        "brand",
        "techSize",
        "incomeID",
        "isSupply",
        "isRealization",
        "totalPrice",
        "discountPercent",
        "spp",
        "finishedPrice",
        "priceWithDisc",
        "isCancel",
        "cancelDate",
        "sticker",
        "gNumber",
        "srid",
    ]

    # --- Подключение к базе и создание плоской таблицы ---
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in ORDER_FIELDS])
    cursor.execute("DROP TABLE IF EXISTS OrdersWBFlat;")
    cursor.execute(
        f"""
    CREATE TABLE OrdersWBFlat (
        org_id INTEGER,
        Организация TEXT,
        {fields_sql},
        PRIMARY KEY (org_id, srid)
    );
    """
    )
    conn.commit()

    # --- HTTP session ---
    def make_http() -> requests.Session:
        s = requests.Session()
        retries = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        s.mount("https://", HTTPAdapter(max_retries=retries))
        s.mount("http://", HTTPAdapter(max_retries=retries))
        return s

    # --- API запрос ---
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers_template = {"Content-Type": "application/json"}

    http = make_http()

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
                resp = http.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
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
                print("✅ Все заказы загружены для этой организации.")
                break

            # Распаковка
            rows = []
            for rec in data:
                flat = [org_id, org_name] + [str(rec.get(f, "")) for f in ORDER_FIELDS]
                rows.append(flat)
            try:
                placeholders = ",".join(["?"] * (2 + len(ORDER_FIELDS)))
                cursor.executemany(
                    f"""
                    INSERT OR REPLACE INTO OrdersWBFlat
                    VALUES ({placeholders})
                """,
                    rows,
                )
                conn.commit()
            except Exception as e:
                print(f"\n  ⚠️ Ошибка вставки: {e}")
                break

            total_loaded += len(rows)
            print(f"  +{len(rows)} заказов (итого: {total_loaded})")

            if len(rows) < PAGE_LIMIT:
                print("  ✅ Заказы по периоду загружены полностью.")
                break

            # pagination: следующий dateFrom = lastChangeDate последней строки
            date_from = data[-1].get("lastChangeDate")
            page += 1
            time.sleep(3)  # Лимит 1 запрос в минуту, но WB часто разрешает чуть чаще

    conn.close()
    print("\n✅ Все заказы загружены и распарсены в таблицу OrdersWBFlat (без дублей).")


if __name__ == "__main__":
    main()
