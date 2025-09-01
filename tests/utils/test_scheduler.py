import sys
from pathlib import Path

import pytest

# Ensure src is importable
sys.path.append(str(Path(__file__).resolve().parents[2] / "src"))

from finmodel.utils.scheduler import schedule_after_meal


def dummy_callback() -> None:  # pragma: no cover - simple placeholder
    pass


@pytest.mark.parametrize("minutes_after", [0, -5, "ten", None])
def test_schedule_after_meal_invalid(minutes_after, caplog):
    caplog.set_level("ERROR")
    timer = schedule_after_meal(dummy_callback, minutes_after)  # type: ignore[arg-type]
    assert timer is None
    assert "minutes_after" in caplog.text
