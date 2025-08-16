from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from finmodel.logger import get_logger

_config: Dict[str, Any] | None = None
logger = get_logger(__name__)


def load_config(path: str | Path | None = None) -> Dict[str, Any]:
    """Load configuration from a YAML file or environment variables.

    The search order is:
    1. ``path`` if provided;
    2. ``FINMODEL_CONFIG`` environment variable;
    3. ``config.yml`` in the project root.

    Loaded values are cached for subsequent calls.
    """
    global _config
    if _config is None:
        base_dir = Path(__file__).resolve().parents[3]
        cfg_path = Path(path or os.getenv("FINMODEL_CONFIG", base_dir / "config.yml"))
        data: Dict[str, Any] = {}
        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        _config = data
    return _config


def find_setting(name: str, default: Any | None = None) -> Any:
    """Return configuration value by ``name``.

    Environment variables take precedence over the ``settings`` section of
    the config file. If the key is missing, ``default`` is returned.
    """
    cfg = load_config().get("settings", {})
    return os.getenv(name) or cfg.get(name, default)


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


def load_organizations(path: str | Path | None = None, sheet: str | None = None) -> pd.DataFrame:
    """Load organizations and tokens from an Excel workbook.

    The workbook ``Настройки.xlsm`` is expected to live in the project root.
    If ``path`` is provided, it overrides the default location. The sheet name
    is looked up via :func:`find_setting` using ``ORG_SHEET`` and defaults to
    ``"НастройкиОрганизаций"``. The returned dataframe contains three columns: ``id``,
    ``Организация`` and ``Token_WB``. Missing or empty rows are dropped.
    """

    sheet = sheet or find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    base_dir = Path(__file__).resolve().parents[3]
    xls_path = Path(path or base_dir / "Настройки.xlsm")
    if not xls_path.exists():
        return pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
    df = df.dropna(how="all")
    if df.empty:
        return pd.DataFrame(columns=["id", "Организация", "Token_WB"])
    header_idx = df.index[0]
    header = df.loc[header_idx].astype(str).str.strip()
    header.name = None
    df = df.loc[df.index > header_idx]
    df.columns = header
    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = df.columns.str.strip()
    required = {"id": "id", "организация": "Организация", "token_wb": "Token_WB"}
    normalized = {c.lower(): c for c in df.columns}
    missing_cols = [required[k] for k in required if k not in normalized]
    if missing_cols:
        logger.warning(
            "Expected columns %s but found %s in organizations workbook %s",
            list(required.values()),
            list(df.columns),
            xls_path,
        )
        return pd.DataFrame(columns=list(required.values()))
    rename_map = {normalized[k]: v for k, v in required.items()}
    df = df.rename(columns=rename_map)
    return df[list(required.values())].dropna()
