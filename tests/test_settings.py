import datetime
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from finmodel.utils.settings import parse_date


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
