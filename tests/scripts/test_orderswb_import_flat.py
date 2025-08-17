import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.scripts import orderswb_import_flat


def test_orderswb_respects_sheet_env(monkeypatch, caplog):
    monkeypatch.setenv("ORG_SHEET", "OrgSheet")
    monkeypatch.setenv("SETTINGS_SHEET", "SettingsSheet")

    load_org = MagicMock(return_value=pd.DataFrame())
    load_period = MagicMock(return_value=("2024-01-01", "2024-01-02"))
    monkeypatch.setattr(orderswb_import_flat, "load_organizations", load_org)
    monkeypatch.setattr(orderswb_import_flat, "load_period", load_period)
    monkeypatch.setattr(orderswb_import_flat, "parse_date", lambda x: pd.Timestamp(x))
    monkeypatch.setattr(orderswb_import_flat.sqlite3, "connect", MagicMock())

    with caplog.at_level("INFO"):
        with pytest.raises(SystemExit):
            orderswb_import_flat.main()

    load_org.assert_called_once_with(sheet="OrgSheet")
    load_period.assert_called_once_with(sheet="SettingsSheet")
    assert "Using organizations sheet OrgSheet" in caplog.text
    assert "Using settings sheet SettingsSheet" in caplog.text
