import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

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
                        "vendorCode": "VC-123",
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
        assert params == {"filterNmID": "123", "limit": script.PAGE_LIMIT}
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
    assert row[1] == "VC-123"
    assert row[2] == "1"
    assert row[3] == 1000
    assert row[4] == 900
    assert row[5] == 10
    assert row[6] == 1000
    assert row[7] == 900
    assert row[8] == pytest.approx(10)
    assert row[9] == pytest.approx(0)
    assert row[10].startswith(datetime.now(timezone.utc).date().isoformat())
    assert row[11] == datetime.now(timezone.utc).date().isoformat()


def test_main_uses_xls_tokens(tmp_path, monkeypatch):
    db = tmp_path / "finmodel.db"
    with sqlite3.connect(db) as conn:

        conn.execute("CREATE TABLE katalog (org_id INTEGER, nmID TEXT)")
        conn.executemany(
            "INSERT INTO katalog (org_id, nmID) VALUES (?, ?)",
            [(1, "111"), (2, "222")],
        )

    monkeypatch.setattr(script, "get_db_path", lambda: db)

    monkeypatch.setattr(
        script, "load_wb_tokens", lambda sheet=None, path=None: [(1, "T1"), (2, "T2")]
    )

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
    monkeypatch.setattr(
        script,
        "calc_metrics",
        lambda r: {
            **r,
            "snapshot_date": "2024-01-01",
            "updated_at_utc": "2024-01-01T00:00:00",
        },
    )
    monkeypatch.setattr(script.time, "sleep", lambda x: None)

    script.main([])

    assert used_tokens == ["T1", "T2"]
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT org_id, nmId FROM WBGoodsPricesFlat ORDER BY org_id").fetchall()
    assert rows == [(1, "111"), (2, "222")]


def test_main_skips_nmids_on_http_error(monkeypatch, caplog):
    nmids = ["111", "222"]
    monkeypatch.setattr(script, "read_nmids_from_txt", lambda p: nmids)
    monkeypatch.setattr(script, "load_wb_tokens", lambda sheet=None, path=None: [(None, "T")])
    monkeypatch.setattr(script, "get_db_path", lambda: "dummy")
    monkeypatch.setattr(script, "make_http", lambda token: SimpleNamespace())

    def fake_fetch_batch(http, nm_id=None, limit=1000, offset=0):
        if nm_id == "111":
            raise requests.exceptions.HTTPError("boom")
        return [
            {
                "nmId": nm_id,
                "sizeID": None,
                "price": 1,
                "discountedPrice": 1,
                "discount": 0,
            }
        ]

    monkeypatch.setattr(script, "fetch_batch", fake_fetch_batch)
    monkeypatch.setattr(
        script,
        "calc_metrics",
        lambda r: {
            **r,
            "snapshot_date": "2024-01-01",
            "updated_at_utc": "2024-01-01T00:00:00",
        },
    )
    monkeypatch.setattr(script.time, "sleep", lambda x: None)

    collected = []
    monkeypatch.setattr(script, "write_prices_to_db", lambda db_path, rows: collected.extend(rows))

    with caplog.at_level(logging.WARNING):
        script.main(["--txt", "dummy"])

    assert any("HTTP error for nmID 111" in r.message for r in caplog.records)
    assert collected and collected[0]["nmId"] == "222"


def test_write_prices_no_duplicates_same_day(tmp_path):
    db = tmp_path / "finmodel.db"
    today = datetime.now(timezone.utc).date().isoformat()
    row = {
        "org_id": 1,
        "nmId": "123",
        "vendorCode": "VC-123",
        "sizeID": "1",
        "price": 1000,
        "discountedPrice": 900,
        "discount": 10,
        "price_rub": 1000,
        "salePrice_rub": 900,
        "discount_total_pct": 10.0,
        "spp_pct_approx": 0.0,
        "snapshot_date": today,
    }
    script.write_prices_to_db(str(db), [row])
    script.write_prices_to_db(str(db), [row])
    with sqlite3.connect(db) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM WBGoodsPricesFlat WHERE snapshot_date = ?",
            (today,),
        ).fetchone()[0]
    assert count == 1
