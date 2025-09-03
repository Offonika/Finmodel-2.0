from __future__ import annotations

import os
from datetime import datetime, tzinfo
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from finmodel.logger import get_logger
from finmodel.utils.paths import get_project_root

_config: Dict[str, Any] | None = None
_config_path: Path | None = None
logger = get_logger(__name__)


def load_config(path: str | Path | None = None, force_reload: bool = False) -> Dict[str, Any]:
    """Load configuration from a YAML file or environment variables.

    The search order is:
    1. ``path`` if provided;
    2. ``FINMODEL_CONFIG`` environment variable;
    3. ``config.yml`` in the project root.

    Loaded values are cached for subsequent calls. Set ``force_reload`` to
    ``True`` or pass a new ``path`` to reload the configuration.
    """
    global _config, _config_path
    base_dir = get_project_root()
    cfg_path = Path(path or os.getenv("FINMODEL_CONFIG", base_dir / "config.yml"))
    if force_reload or _config is None or _config_path != cfg_path:
        data: Dict[str, Any] = {}
        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        _config = data
        _config_path = cfg_path
    return _config


def find_setting(name: str, default: Any | None = None) -> Any:
    """Return configuration value by ``name``.

    Environment variables take precedence over the ``settings`` section of
    the config file. If the key is missing, ``default`` is returned.
    """
    cfg = load_config(force_reload=True).get("settings", {})
    return os.getenv(name) or cfg.get(name, default)


def parse_date(dt, tz: str | tzinfo | None = None) -> pd.Timestamp:
    """Parse various date formats into a timezone-aware ``pd.Timestamp``.

    Supports ``dd.mm.yyyy``, ``yyyy-mm-dd`` and arbitrary ISO-like formats.
    The returned timestamp is in UTC unless ``tz`` is provided to convert it to
    another timezone.
    """
    s = str(dt).replace("T", " ").replace("/", ".").strip()
    if s == "":
        raise ValueError("empty date")
    ts = pd.to_datetime(s, utc=True, dayfirst="." in s)
    if tz is not None:
        ts = ts.tz_convert(tz)
    return ts


def load_organizations(path: str | Path | None = None, sheet: str | None = None) -> pd.DataFrame:
    """Load organizations and tokens from an Excel workbook.

    The workbook ``Настройки.xlsm`` is expected to live in the project root.
    If ``path`` is provided, it overrides the default location. The sheet name
    is looked up via :func:`find_setting` using ``ORG_SHEET`` and defaults to
    ``"НастройкиОрганизаций"``. The returned dataframe contains three columns: ``id``,
    ``Организация`` and ``Token_WB``. Missing or empty rows are dropped.
    """

    sheet = sheet or find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    base_dir = get_project_root()
    xls_path = Path(path or base_dir / "Настройки.xlsm")
    logger.info("Loading organizations from %s sheet %s", xls_path, sheet)
    if not xls_path.exists():
        logger.warning("Workbook %s not found", xls_path)
        return pd.DataFrame(columns=["id", "Организация", "Token_WB"])

    with pd.ExcelFile(xls_path) as xls:
        logger.debug("Available sheets in %s: %s", xls_path, xls.sheet_names)
        if sheet not in xls.sheet_names:
            logger.warning(
                "Sheet %s not found in %s. Available sheets: %s", sheet, xls_path, xls.sheet_names
            )
            return pd.DataFrame(columns=["id", "Организация", "Token_WB"])

        df = xls.parse(sheet_name=sheet, header=None)
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
        logger.debug("Data head for debugging:\n%s", df.head().to_string())
        return pd.DataFrame(columns=list(required.values()))

    rename_map = {normalized[k]: v for k, v in required.items()}
    df = df.rename(columns=rename_map)
    df = df[list(required.values())].dropna()
    logger.info("Loaded %d organizations", len(df))
    return df


def load_period(
    path: str | Path | None = None, sheet: str | None = None
) -> tuple[str | None, str | None]:
    """Load ``ПериодНачало`` and ``ПериодКонец`` from ``Настройки.xlsm``.

    The workbook sheet is looked up via :func:`find_setting` using the
    ``SETTINGS_SHEET`` key and defaults to ``"Настройки"``. The sheet is
    expected to contain two columns: ``Параметр`` and ``Значение``. Rows where
    either column is blank (e.g. section headers) are ignored. The function
    searches the table for ``ПериодНачало`` and ``ПериодКонец`` parameters and
    returns their string values. If either parameter is missing, ``(None, None)``
    is returned and a warning is logged.
    """

    sheet = sheet or find_setting("SETTINGS_SHEET", default="Настройки")
    base_dir = get_project_root()
    xls_path = Path(path or base_dir / "Настройки.xlsm")
    if not xls_path.exists():
        logger.warning("Workbook %s not found", xls_path)
        return None, None

    df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
    df = df.dropna(how="all")
    if df.empty:
        return None, None

    header_idx = df.index[0]
    header = df.loc[header_idx].astype(str).str.strip()
    df = df.loc[df.index > header_idx]
    df.columns = header
    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = df.columns.map(lambda c: str(c).strip())

    required = {"параметр": "Параметр", "значение": "Значение"}
    normalized = {c.lower(): c for c in df.columns}
    if not all(key in normalized for key in required):
        logger.warning(
            "Expected columns %s but found %s in settings workbook %s",
            list(required.values()),
            list(df.columns),
            xls_path,
        )
        return None, None

    rename_map = {normalized[k]: v for k, v in required.items()}
    df = df.rename(columns=rename_map)
    df = df[["Параметр", "Значение"]]

    values: Dict[str, str] = {}
    for _, row in df.iterrows():
        key = row.get("Параметр")
        value = row.get("Значение")
        if pd.isna(key) or pd.isna(value) or str(key).strip() == "" or str(value).strip() == "":
            continue
        values[str(key).strip()] = str(value).strip()

    start = values.get("ПериодНачало")
    end = values.get("ПериодКонец")
    if start is None or end is None:
        logger.warning(
            "Parameters 'ПериодНачало'/'ПериодКонец' not found in %s sheet %s",
            xls_path,
            sheet,
        )
        return None, None
    return start, end


def load_global_settings(
    path: str | Path | None = None, sheet: str | None = None
) -> Dict[str, str]:
    """Load arbitrary ``Параметр``/``Значение`` pairs from ``Настройки.xlsm``.

    The workbook sheet is looked up via :func:`find_setting` using the
    ``SETTINGS_SHEET`` key and defaults to ``"Настройки"``. The function returns a
    dictionary mapping normalized parameter names (trimmed and lower-cased) to
    their string values. Blank rows or cells containing only whitespace are
    ignored.
    """

    sheet = sheet or find_setting("SETTINGS_SHEET", default="Настройки")
    base_dir = get_project_root()
    xls_path = Path(path or base_dir / "Настройки.xlsm")
    if not xls_path.exists():
        return {}

    df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
    df = df.dropna(how="all")
    if df.empty:
        return {}

    header_idx = df.index[0]
    header = df.loc[header_idx].astype(str).str.strip()
    df = df.loc[df.index > header_idx]
    df.columns = header
    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = df.columns.map(lambda c: str(c).strip())

    required = {"параметр": "Параметр", "значение": "Значение"}
    normalized = {c.lower(): c for c in df.columns}
    if not all(key in normalized for key in required):
        return {}

    rename_map = {normalized[k]: v for k, v in required.items()}
    df = df.rename(columns=rename_map)
    df = df[["Параметр", "Значение"]]

    result: Dict[str, str] = {}
    for _, row in df.iterrows():
        key = row.get("Параметр")
        value = row.get("Значение")
        if pd.isna(key) or pd.isna(value):
            continue
        key = str(key).strip()
        value = str(value).strip()
        if not key or not value:
            continue
        result[key.lower()] = value
    return result
