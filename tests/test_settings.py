import datetime
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from finmodel.utils.settings import load_organizations, parse_date


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
