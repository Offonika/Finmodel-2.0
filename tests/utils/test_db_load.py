import sqlite3
from pathlib import Path

from finmodel.utils.db_load import load_wb_tokens


def test_load_wb_tokens(tmp_path):
    db = tmp_path / "finmodel.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE НастройкиОрганизаций (id INTEGER, Token_WB TEXT)")
        conn.executemany(
            "INSERT INTO НастройкиОрганизаций (id, Token_WB) VALUES (?, ?)",
            [(1, "AAA"), (2, None), (3, "BBB")],
        )
    tokens = load_wb_tokens(db)
    assert tokens == [(1, "AAA"), (3, "BBB")]
