import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import katalog


def test_katalog_handles_missing_columns(monkeypatch, caplog):
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    monkeypatch.setattr(katalog, "load_organizations", lambda **_: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("INFO"):
        katalog.main()
    assert "Using organizations sheet" in caplog.text
    assert "missing required columns" in caplog.text
    connect.assert_not_called()


def test_katalog_handles_empty_dataframe(monkeypatch, caplog):
    df = pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    monkeypatch.setattr(katalog, "load_organizations", lambda **_: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("INFO"):
        katalog.main()
    assert "Using organizations sheet" in caplog.text
    assert "не содержит организаций" in caplog.text
    connect.assert_not_called()
