#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обновляет таблицу wb_spp: берёт nmID из katalog, запрашивает
https://card.wb.ru/cards/v4/detail и сохраняет цены/скидки.

Запуск по умолчанию ищет finmodel.db на уровень выше
каталога, где лежит скрипт. Можно задать другой путь:
    python wb_spp_fetch.py --db "C:\\path\\to\\finmodel.db"
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple

import requests

# ──────────────────────────────────────────────────────────────────────────────
# 1. Параметры и путь к базе
# ──────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db",
        metavar="PATH",
        help="Полный путь к finmodel.db (по умолчанию: ../finmodel.db).",
    )
    parser.add_argument("-h", "--help", action="help", help="Показать эту справку.")
    return parser.parse_args()


def resolve_db_path(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path).expanduser().resolve()
    #  …/scriptsPB/wb_spp_fetch.py → …/finmodel.db
    return Path(__file__).resolve().parent.parent / "finmodel.db"


args = parse_args()
DB_PATH: Path = resolve_db_path(args.db)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Константы API
# ──────────────────────────────────────────────────────────────────────────────
API_URL = (
    "https://card.wb.ru/cards/v4/detail"
    "?appType=1&curr=rub&dest=-1257786&spp=0&nm={nm}"
)
REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_CALLS = 0.2
BATCH_SIZE = 100

# ──────────────────────────────────────────────────────────────────────────────
# 3. SQL
# ──────────────────────────────────────────────────────────────────────────────
CREATE_WB_SPP_SQL = """
CREATE TABLE IF NOT EXISTS wb_spp (
    nmID         INTEGER PRIMARY KEY,
    priceU       INTEGER NOT NULL,
    salePriceU   INTEGER NOT NULL,
    sale_pct     INTEGER NOT NULL,
    spp          INTEGER,            -- пока редко приходит → может быть NULL
    updated_at   TEXT    NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO wb_spp (nmID, priceU, salePriceU, sale_pct, spp, updated_at)
VALUES (?, ?, ?, ?, ?, datetime('now'))
ON CONFLICT(nmID) DO UPDATE SET
    priceU       = excluded.priceU,
    salePriceU   = excluded.salePriceU,
    sale_pct     = excluded.sale_pct,
    spp          = excluded.spp,
    updated_at   = excluded.updated_at;
"""

# ──────────────────────────────────────────────────────────────────────────────
# 4. Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────
def get_nm_ids(cur) -> list[int]:
    cur.execute("SELECT nmID FROM katalog")
    return [row[0] for row in cur.fetchall()]


def fetch_card(nm_id: int) -> Tuple[int, int, int, int, Optional[int]]:
    r = requests.get(API_URL.format(nm=nm_id), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    product = r.json()["products"][0]
    sizes = product.get("sizes", [])
    if not sizes:
        raise ValueError(f"no sizes data for nmID {nm_id}")

    pb = sizes[0]["price"]
    priceU, salePriceU = pb["basic"], pb["product"]
    sale_pct = round((priceU - salePriceU) / priceU * 100)
    spp = pb.get("spp")  # почти всегда None

    return nm_id, priceU, salePriceU, sale_pct, spp


# ──────────────────────────────────────────────────────────────────────────────
# 5. Основной поток
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    print("Используем базу:", DB_PATH)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(CREATE_WB_SPP_SQL)
    con.commit()

    try:
        nm_ids = get_nm_ids(cur)
    except sqlite3.OperationalError:
        print("❌ Таблица katalog не найдена. Создайте её и заполните nmID.")
        return

    print(f"Всего nmID: {len(nm_ids)}")

    batch: list[tuple[int, int, int, int, Optional[int]]] = []
    for i, nm in enumerate(nm_ids, 1):
        try:
            row = fetch_card(nm)
        except Exception as e:
            print(f"[{i}/{len(nm_ids)}] nmID={nm} ❌ {e}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            cur.executemany(INSERT_SQL, batch)
            con.commit()
            batch.clear()

        print(
            f"[{i}/{len(nm_ids)}] nmID={row[0]} "
            f"priceU={row[1]} salePriceU={row[2]} sale%={row[3]} spp={row[4]}"
        )
        time.sleep(SLEEP_BETWEEN_CALLS)

    if batch:
        cur.executemany(INSERT_SQL, batch)
        con.commit()

    con.close()
    print("Готово.")


if __name__ == "__main__":
    main()
