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
        df.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False)
    return xls


@pytest.fixture
def mock_read_excel_missing(monkeypatch, tmp_path):
    """Patch pd.ExcelFile to simulate missing columns."""

    class FakeExcelFile:
        sheet_names = ["НастройкиОрганизаций"]

        def parse(self, *_, **__):
            return pd.DataFrame({0: ["id", 1]})

    monkeypatch.setattr(pd, "ExcelFile", lambda *_, **__: FakeExcelFile())
    xls = tmp_path / "orgs.xlsx"
    xls.touch()
    return xls


@pytest.fixture
def excel_mixed_headers(tmp_path):
    """Create an Excel file with spaced and case-varied headers."""
    df = pd.DataFrame({"ID": [1], "Организация ": ["Org"], "token_wb": ["tok"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False)
    return xls


@pytest.fixture
def excel_with_blank_rows(tmp_path):
    """Create an Excel file with leading blank rows."""
    df = pd.DataFrame({"id": [1], "Организация": ["Org"], "Token_WB": ["tok"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False, startrow=2)
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


def test_load_organizations_skips_leading_blank_rows(excel_with_blank_rows):
    df = load_organizations(excel_with_blank_rows)
    expected = pd.DataFrame({"id": [1], "Организация": ["Org"], "Token_WB": ["tok"]})
    pd.testing.assert_frame_equal(df, expected, check_dtype=False)


def test_load_organizations_uses_env_sheet(tmp_path, monkeypatch):
    df_default = pd.DataFrame({"id": [1], "Организация": ["A"], "Token_WB": ["def"]})
    df_custom = pd.DataFrame({"id": [2], "Организация": ["B"], "Token_WB": ["tok"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df_default.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False)
        df_custom.to_excel(writer, sheet_name="Custom", index=False)
    monkeypatch.setenv("ORG_SHEET", "Custom")
    loaded = load_organizations(xls)
    pd.testing.assert_frame_equal(loaded, df_custom, check_dtype=False)


def test_load_organizations_logs_path_sheet_and_count(excel_with_blank_rows, caplog):
    with caplog.at_level("INFO", logger="finmodel.utils.settings"):
        df = load_organizations(excel_with_blank_rows)
    assert str(excel_with_blank_rows) in caplog.text
    assert "sheet НастройкиОрганизаций" in caplog.text
    assert "Loaded 1 organizations" in caplog.text
    assert not df.empty


def test_load_organizations_warns_on_missing_workbook(tmp_path, caplog):
    missing = tmp_path / "no.xlsx"
    with caplog.at_level("INFO", logger="finmodel.utils.settings"):
        df = load_organizations(missing)
    assert df.empty
    assert f"Loading organizations from {missing}" in caplog.text
    assert "Workbook" in caplog.text and "not found" in caplog.text


def test_load_organizations_logs_available_sheets_when_sheet_missing(excel_mixed_headers, caplog):
    with caplog.at_level("DEBUG", logger="finmodel.utils.settings"):
        load_organizations(excel_mixed_headers, sheet="Missing")
    assert "Available sheets" in caplog.text
    assert "Sheet Missing not found" in caplog.text


def test_load_organizations_logs_expected_and_actual_columns(excel_missing_token, caplog):
    with caplog.at_level("DEBUG", logger="finmodel.utils.settings"):
        load_organizations(excel_missing_token)
    assert "['id', 'Организация', 'Token_WB']" in caplog.text
    assert "['id', 'Организация']" in caplog.text
    assert "Data head for debugging" in caplog.text


def test_katalog_handles_missing_columns(monkeypatch, caplog):
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    monkeypatch.setattr(katalog, "load_organizations", lambda **_: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("ERROR"):
        katalog.main()
    assert "missing required columns" in caplog.text
    connect.assert_not_called()


def test_katalog_handles_empty_dataframe(monkeypatch, caplog):
    df = pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    monkeypatch.setattr(katalog, "load_organizations", lambda **_: df)
    connect = MagicMock()
    monkeypatch.setattr(katalog.sqlite3, "connect", connect)
    with caplog.at_level("ERROR"):
        katalog.main()
    assert "не содержит организаций" in caplog.text
    connect.assert_not_called()
