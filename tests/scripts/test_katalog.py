import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import katalog


def test_katalog_handles_missing_columns(monkeypatch, caplog):
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    load_org = MagicMock(return_value=df)
    monkeypatch.setattr(katalog, "load_organizations", load_org)
    monkeypatch.setattr(katalog.sqlite3, "connect", MagicMock())
    monkeypatch.setenv("ORG_SHEET", "CustomOrg")
    monkeypatch.setenv("SETTINGS_SHEET", "CustomSettings")
    with caplog.at_level("INFO"):
        katalog.main()
    load_org.assert_called_once_with(sheet="CustomOrg")
    assert "Using organizations sheet: CustomOrg" in caplog.text
    assert "Using settings sheet CustomSettings" in caplog.text
    assert "missing required columns" in caplog.text


def test_katalog_handles_empty_dataframe(monkeypatch, caplog):
    df = pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    load_org = MagicMock(return_value=df)
    monkeypatch.setattr(katalog, "load_organizations", load_org)
    monkeypatch.setattr(katalog.sqlite3, "connect", MagicMock())
    monkeypatch.setenv("ORG_SHEET", "SheetX")
    monkeypatch.setenv("SETTINGS_SHEET", "SheetY")
    with caplog.at_level("INFO"):
        katalog.main()
    load_org.assert_called_once_with(sheet="SheetX")
    assert "Using organizations sheet: SheetX" in caplog.text
    assert "Using settings sheet SheetY" in caplog.text
    assert "не содержит организаций" in caplog.text
