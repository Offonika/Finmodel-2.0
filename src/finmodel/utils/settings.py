from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

# Path to the Excel settings file
base_dir = Path(__file__).resolve().parents[3]
xls_path = base_dir / "Finmodel.xlsm"

_df_settings = None


def _load_settings() -> None:
    global _df_settings
    if _df_settings is None:
        _df_settings = pd.read_excel(xls_path, sheet_name="Настройки", engine="openpyxl")


def find_setting(name: str):
    """Return the value of a setting by name from the Excel file."""
    _load_settings()
    val = _df_settings.loc[_df_settings["Параметр"].astype(str).str.strip() == name, "Значение"]
    return val.values[0] if not val.empty else None


def parse_date(dt) -> datetime:
    """Parse various date formats into ``datetime``.

    Supports ``dd.mm.yyyy``, ``yyyy-mm-dd`` and arbitrary ISO-like formats.
    """
    s = str(dt).replace("T", " ").replace("/", ".").strip()
    if s == "":
        raise ValueError("empty date")
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return pd.to_datetime(s).to_pydatetime()
