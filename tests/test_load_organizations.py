import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from finmodel.scripts import katalog
from finmodel.utils.settings import load_organizations


@pytest.fixture
def excel_missing_token(tmp_path):
    """Create an Excel file missing the Token_WB column."""
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False)
    return xls


@pytest.fixture
def mock_read_excel_missing(monkeypatch, tmp_path):
    """Patch pd.read_excel to simulate missing columns."""

    def fake_read_excel(*args, **kwargs):
        return pd.DataFrame({"id": [1]})

    monkeypatch.setattr(pd, "read_excel", fake_read_excel)
    # Ensure the path exists so load_organizations proceeds to read_excel
    xls = tmp_path / "orgs.xlsx"
    xls.touch()
    return xls


@pytest.fixture
def excel_mixed_headers(tmp_path):
    """Create an Excel file with spaced and case-varied headers."""
    df = pd.DataFrame({"ID": [1], "Организация ": ["Org"], "token_wb": ["tok"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False)
    return xls


def test_load_organizations_with_missing_column(excel_missing_token):
    df = load_organizations(excel_missing_token)
    assert list(df.columns) == ["id", "Организация", "Token_WB"]
    assert df.empty


def test_load_organizations_with_mocked_read_excel(mock_read_excel_missing):
    df = load_organizations(mock_read_excel_missing)
    assert list(df.columns) == ["id", "Организация", "Token_WB"]
    assert df.empty


def test_load_organizations_normalizes_headers(excel_mixed_headers):
    df = load_organizations(excel_mixed_headers)
    assert list(df.columns) == ["id", "Организация", "Token_WB"]
    assert df.loc[0, "Token_WB"] == "tok"


def test_katalog_handles_missing_columns(monkeypatch, caplog):
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    monkeypatch.setattr(katalog, "load_organizations", lambda: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("ERROR"):
        katalog.main()
    assert "missing required columns" in caplog.text
    connect.assert_not_called()


def test_katalog_handles_empty_dataframe(monkeypatch, caplog):
    df = pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    monkeypatch.setattr(katalog, "load_organizations", lambda: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("ERROR"):
        katalog.main()
    assert "не содержит организаций" in caplog.text
    connect.assert_not_called()
