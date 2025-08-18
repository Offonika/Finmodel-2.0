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
            "data": {"products": [{"id": 123, "priceU": 1000, "salePriceU": 900, "sale": 10}]}
        },
    )
    fake_session = SimpleNamespace(get=lambda url, params, timeout: fake_response)

    rows: list[tuple] = []

    class FakeCursor:
        fast_executemany = False

        def executemany(self, sql, batch):
            rows.extend(batch)

        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_pyodbc = SimpleNamespace(connect=lambda dsn, autocommit=True: FakeConn())
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    monkeypatch.setattr(script.time, "sleep", lambda x: None)

    inserted = script.import_prices(["123"], dsn="DSN=test", http=fake_session)

    assert inserted == 1
    assert rows[0][0] == "123"
