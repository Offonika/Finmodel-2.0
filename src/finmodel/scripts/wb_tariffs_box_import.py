import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from finmodel.logger import get_logger
from finmodel.utils.settings import find_setting, load_organizations, load_period, parse_date

logger = get_logger(__name__)


def main() -> None:
    # --- Paths ---
    base_dir = Path(__file__).resolve().parents[3]
    db_path = base_dir / "finmodel.db"

    logger.info("DB: %s", db_path)

    # --- Дата запроса: берём из конфигурации (ПериодКонец), иначе сегодня ---
    _, date_raw = load_period()
    if date_raw:
        date_param = parse_date(date_raw).strftime("%Y-%m-%d")
    else:
        date_param = datetime.now().strftime("%Y-%m-%d")
    logger.info("Дата для запроса тарифов: %s", date_param)

    # --- Load tokens (try each until one works) ---
    sheet = find_setting("ORG_SHEET", default="Настройки")
    df_orgs = load_organizations(sheet=sheet)
    tokens = df_orgs["Token_WB"].dropna().astype(str).map(str.strip).tolist()

    if not tokens:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
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
            logger.info("  Тест токена → HTTP %s", r.status_code)
            if r.status_code != 200:
                logger.warning("  Ответ: %s", r.text[:300])
                return None
            return r.json()
        except Exception as e:
            logger.warning("  Ошибка сети: %s", e)
            return None

    data = None
    for i, tk in enumerate(tokens, 1):
        logger.info("\nПробую токен №%s ...", i)
        data = try_fetch(tk)
        if data:
            logger.info("  ✅ Данные получены.")
            break

    if not data:
        logger.error("Не удалось получить тарифы ни с одним токеном. Проверьте сеть/доступ/токены.")
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
        logger.warning("Список складов пуст. Возможно, на эту дату тарификация не определена.")
    else:
        placeholders = ",".join(["?"] * len(FIELDS))
        cur.executemany(f"INSERT INTO {TABLE} VALUES ({placeholders})", rows)
        conn.commit()
        logger.info("✅ Вставлено строк: %s в %s", len(rows), TABLE)

    conn.close()
    logger.info("Готово.")


if __name__ == "__main__":
    main()
