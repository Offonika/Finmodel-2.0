import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from finmodel.utils.settings import find_setting, load_config, parse_date


def main(config=None):
    config = config or load_config()
    # --- Пути ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = Path(config.get("db_path", base_dir / "finmodel.db"))

    print(f"DB:  {db_path}")

    # --- Дата запроса: берём из конфигурации (ПериодКонец), иначе сегодня ---
    date_raw = find_setting("ПериодКонец")
    if date_raw:
        date_param = parse_date(date_raw).strftime("%Y-%m-%d")
    else:
        date_param = datetime.now().strftime("%Y-%m-%d")
    print(f"Дата для запроса тарифов: {date_param}")

    # --- Чтение токенов (перебор до первого рабочего) ---
    df_orgs = pd.DataFrame(config.get("organizations", []))
    tokens = df_orgs.get("Token_WB", pd.Series()).dropna().astype(str).map(str.strip).tolist()

    if not tokens:
        print("❗ Не найдено ни одного токена в 'НастройкиОрганизаций'.")
        raise SystemExit(1)

    # --- Итоговая таблица (пересоздаём) ---
    TABLE = "WBTariffsBox"
    FIELDS = [
        "DateParam",  # дата, по которой запрашивали тарифы
        "dtNextBox",
        "dtTillMax",
        "warehouseName",
        "geoName",
        "boxDeliveryAndStorageExpr",
        "boxDeliveryBase",
        "boxDeliveryCoefExpr",
        "boxDeliveryLiter",
        "boxDeliveryMarketplaceBase",
        "boxDeliveryMarketplaceCoefExpr",
        "boxDeliveryMarketplaceLiter",
        "boxStorageBase",
        "boxStorageCoefExpr",
        "boxStorageLiter",
        "LoadDate",
    ]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
    cur.execute(
        f"""
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
    """
    )
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
    dat = resp.get("data", {})
    dtNextBox = str(dat.get("dtNextBox", ""))
    dtTillMax = str(dat.get("dtTillMax", ""))
    warehouses = dat.get("warehouseList", []) or []

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for wh in warehouses:
        rows.append(
            [
                date_param,
                dtNextBox,
                dtTillMax,
                str(wh.get("warehouseName", "")),
                str(wh.get("geoName", "")),
                str(wh.get("boxDeliveryAndStorageExpr", "")),
                str(wh.get("boxDeliveryBase", "")),
                str(wh.get("boxDeliveryCoefExpr", "")),
                str(wh.get("boxDeliveryLiter", "")),
                str(wh.get("boxDeliveryMarketplaceBase", "")),
                str(wh.get("boxDeliveryMarketplaceCoefExpr", "")),
                str(wh.get("boxDeliveryMarketplaceLiter", "")),
                str(wh.get("boxStorageBase", "")),
                str(wh.get("boxStorageCoefExpr", "")),
                str(wh.get("boxStorageLiter", "")),
                now_str,
            ]
        )

    if not rows:
        print("⚠️ Список складов пуст. Возможно, на эту дату тарификация не определена.")
    else:
        placeholders = ",".join(["?"] * len(FIELDS))
        cur.executemany(f"INSERT INTO {TABLE} VALUES ({placeholders})", rows)
        conn.commit()
        print(f"✅ Вставлено строк: {len(rows)} в {TABLE}")

    conn.close()
    print("\nГотово.")


if __name__ == "__main__":
    main()
