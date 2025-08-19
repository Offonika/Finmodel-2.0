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
