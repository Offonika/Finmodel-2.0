from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

_config: Dict[str, Any] | None = None


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
