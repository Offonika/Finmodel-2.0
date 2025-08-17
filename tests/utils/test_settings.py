import datetime
import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))
import finmodel.utils.settings as settings
from finmodel.utils.settings import (
    load_global_settings,
    load_organizations,
    load_period,
    parse_date,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("01.02.2023", datetime.datetime(2023, 2, 1)),
        ("2023-02-01", datetime.datetime(2023, 2, 1)),
        ("2023-02-01T13:45:00", datetime.datetime(2023, 2, 1, 13, 45, 0)),
    ],
)
def test_parse_date(raw, expected):
    assert parse_date(raw) == expected


def test_load_organizations_missing_columns(tmp_path):
    df = pd.DataFrame({"id": [1], "Организация": ["Org"]})
    xls = tmp_path / "orgs.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="НастройкиОрганизаций", index=False)
    result = load_organizations(xls)
    assert list(result.columns) == ["id", "Организация", "Token_WB"]
    assert result.empty


def test_load_period_skips_blank_rows(tmp_path):
    df = pd.DataFrame({"ПериодНачало": ["2023-01-01"], "ПериодКонец": ["2023-01-31"]})
    xls = tmp_path / "settings.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False, startrow=2)
    start, end = load_period(xls, sheet="Настройки")
    assert start == "2023-01-01"
    assert end == "2023-01-31"


def test_load_period_missing_columns(tmp_path):
    df = pd.DataFrame({"foo": [1]})
    xls = tmp_path / "settings.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False)
    start, end = load_period(xls, sheet="Настройки")
    assert start is None and end is None


def test_load_period_custom_sheet(tmp_path, monkeypatch):
    df = pd.DataFrame({"ПериодНачало": ["2023-01-01"], "ПериодКонец": ["2023-01-31"]})
    xls = tmp_path / "settings.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Другое", index=False)
    monkeypatch.setenv("SETTINGS_SHEET", "Другое")
    monkeypatch.setattr(settings, "_config", None)
    start, end = load_period(xls)
    assert start == "2023-01-01" and end == "2023-01-31"


def test_load_global_settings_basic(tmp_path):
    df = pd.DataFrame({"Параметр": [" Foo", "Bar "], "Значение": [" 1", "2 "]})
    xls = tmp_path / "settings.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False, startrow=1)
    result = load_global_settings(xls, sheet="Настройки")
    assert result == {"foo": "1", "bar": "2"}


def test_load_global_settings_skips_blank(tmp_path):
    df = pd.DataFrame(
        {
            "Параметр": [" foo ", " ", None, "BAR"],
            "Значение": [" val1 ", "", "skip", " 42 "],
        }
    )
    xls = tmp_path / "settings.xlsx"
    with pd.ExcelWriter(xls) as writer:
        df.to_excel(writer, sheet_name="Настройки", index=False, startrow=2)
    result = load_global_settings(xls, sheet="Настройки")
    assert result == {"foo": "val1", "bar": "42"}
