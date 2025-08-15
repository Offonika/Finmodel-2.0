import sqlite3
from pathlib import Path

import pandas as pd
import requests

from finmodel.utils.settings import load_config


def main(config=None):
    config = config or load_config()
    # --- Пути ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = Path(config.get("db_path", base_dir / "finmodel.db"))

    # --- Чтение всех токенов ---
    df_orgs = pd.DataFrame(config.get("organizations", []))
    tokens = df_orgs.get("Token_WB", pd.Series()).dropna().astype(str).tolist()
    if not tokens:
        print("❗ Конфигурация не содержит токенов.")
        return

    # --- Поля по документации WB ---
    FIELDS = [
        "kgvpBooking",
        "kgvpMarketplace",
        "kgvpPickup",
        "kgvpSupplier",
        "kgvpSupplierExpress",
        "paidStorageKgvp",
        "parentID",
        "parentName",
        "subjectID",
        "subjectName",
    ]

    # --- Пересоздаём таблицу ---
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    fields_sql = ", ".join([f"{f} TEXT" for f in FIELDS])
    cursor.execute("DROP TABLE IF EXISTS WBTariffsCommission;")
    cursor.execute(
        f"""
    CREATE TABLE WBTariffsCommission (
        {fields_sql}
    );
    """
    )
    conn.commit()

    # --- Пытаемся получить комиссии с каждым токеном ---
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    params = {"locale": "ru"}
    found_data = False

    for idx, token in enumerate(tokens, 1):
        headers = {"Authorization": token}
        print(f"\nПробую токен №{idx} ...", end=" ")
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            if resp.status_code != 200:
                print(f"ответ WB: {resp.status_code}")
                continue
            data = resp.json().get("report", [])
            if not data:
                print("данных нет.")
                continue
            print("Успех!")
            # --- Вставляем плоско ---
            rows = []
            for rec in data:
                flat = [str(rec.get(f, "")) for f in FIELDS]
                rows.append(flat)
            placeholders = ",".join(["?"] * len(FIELDS))
            cursor.executemany(
                f"""
                INSERT INTO WBTariffsCommission
                VALUES ({placeholders})
            """,
                rows,
            )
            conn.commit()
            print(f"Вставлено {len(rows)} строк в таблицу WBTariffsCommission")
            found_data = True
            break
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            continue

    if not found_data:
        print(
            "\n❗ Не удалось получить данные ни с одним токеном. Проверьте права или интернет/домен WB."
        )

    conn.close()
    print("✅ Комиссии по категориям WB: загрузка завершена.")


if __name__ == "__main__":
    main()
