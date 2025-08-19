import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import wb_goods_prices_import_flat as script


def test_import_prices_inserts_rows(monkeypatch):
    fake_response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {
            "data": {
                "products": [
                    {
                        "id": 123,
                        "sizes": [
                            {
                                "sizeId": 1,
                                "price": 1000,
                                "discountedPrice": 900,
                                "clubDiscountedPrice": 850,
                                "discount": 10,
                                "clubDiscount": 5,
                            }
                        ],
                    }
                ]
            }
        },
    )

    def fake_get(url, params, timeout):
        assert params == {"filterNmID": "123"}
        return fake_response

    fake_session = SimpleNamespace(get=fake_get)

    rows: list[tuple] = []

    class FakeCursor:
        fast_executemany = False

        def execute(self, sql):
            pass

        def executemany(self, sql, batch):
            rows.extend(batch)

        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def close(self):
            pass

    fake_pyodbc = SimpleNamespace(connect=lambda dsn, autocommit=True: FakeConn())
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    monkeypatch.setattr(script.time, "sleep", lambda x: None)

    inserted = script.import_prices(["123"], dsn="DSN=test", api_key="TOKEN", http=fake_session)

    assert inserted == 1
    row = rows[0]
    assert row[0] == "123"
    assert row[1] == "1"
    assert row[2] == 1000
    assert row[3] == 900
    assert row[4] == 10
    assert row[5] == 1000
    assert row[6] == 900
    assert row[7] == pytest.approx(10)
    assert row[8] == pytest.approx(0)


def test_main_uses_db_tokens(tmp_path, monkeypatch):
    db = tmp_path / "finmodel.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE НастройкиОрганизаций (id INTEGER, Token_WB TEXT)")
        conn.executemany(
            "INSERT INTO НастройкиОрганизаций (id, Token_WB) VALUES (?, ?)",
            [(1, "T1"), (2, "T2")],
        )
        conn.execute("CREATE TABLE katalog (org_id INTEGER, nmID TEXT)")
        conn.executemany(
            "INSERT INTO katalog (org_id, nmID) VALUES (?, ?)",
            [(1, "111"), (2, "222")],
        )

    monkeypatch.setattr(script, "get_db_path", lambda: db)

    used_tokens: list[str] = []

    def fake_make_http(token):
        used_tokens.append(token)
        return SimpleNamespace()

    def fake_fetch_batch(http, nm_id=None, limit=1000, offset=0):
        return [
            {
                "nmId": nm_id or str(offset),
                "sizeID": None,
                "price": 1,
                "discountedPrice": 1,
                "discount": 0,
            }
        ]

    monkeypatch.setattr(script, "make_http", fake_make_http)
    monkeypatch.setattr(script, "fetch_batch", fake_fetch_batch)
    monkeypatch.setattr(script, "calc_metrics", lambda r: r)
    monkeypatch.setattr(script.time, "sleep", lambda x: None)

    script.main([])

    assert used_tokens == ["T1", "T2"]
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT org_id, nmId FROM WBGoodsPricesFlat ORDER BY org_id").fetchall()
    assert rows == [(1, "111"), (2, "222")]
