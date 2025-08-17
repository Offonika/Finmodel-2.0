from __future__ import annotations

import logging
from typing import Optional

from finmodel.utils.paths import get_project_root

LOG_DIR = get_project_root() / "log"
LOG_FILE = LOG_DIR / "finmodel.log"

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging() -> None:
    """Configure logging for the project."""
    if logging.getLogger().hasHandlers():
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger configured for the project."""
    return logging.getLogger(name)
