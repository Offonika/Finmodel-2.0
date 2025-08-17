from __future__ import annotations

import os
from pathlib import Path


def get_project_root() -> Path:
    """Return the repository root.

    The location can be overridden by the ``FINMODEL_PROJECT_ROOT`` environment
    variable. Otherwise the path is resolved relative to this file.
    """
    env_root = os.getenv("FINMODEL_PROJECT_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def get_db_path() -> Path:
    """Return path to ``finmodel.db``.

    ``FINMODEL_DB_PATH`` environment variable has priority over the default
    location under the project root.
    """
    env_db = os.getenv("FINMODEL_DB_PATH")
    if env_db:
        return Path(env_db).expanduser().resolve()
    return get_project_root() / "finmodel.db"
