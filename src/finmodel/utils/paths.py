from __future__ import annotations

import os
from importlib.resources import files
from pathlib import Path


def get_project_root() -> Path:
    """Return the project root directory.

    The location can be overridden by the ``FINMODEL_PROJECT_ROOT`` environment
    variable. Otherwise the function looks for repository markers such as
    ``.git`` or ``pyproject.toml`` starting from this file's location. If no
    marker is found (e.g. when running from an installed package), the package
    location itself is used.
    """
    env_root = os.getenv("FINMODEL_PROJECT_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent

    return Path(files("finmodel")).resolve()


def get_db_path() -> Path:
    """Return path to ``finmodel.db``.

    ``FINMODEL_DB_PATH`` environment variable has priority over the default
    location under the project root.
    """
    env_db = os.getenv("FINMODEL_DB_PATH")
    if env_db:
        return Path(env_db).expanduser().resolve()
    return get_project_root() / "finmodel.db"
