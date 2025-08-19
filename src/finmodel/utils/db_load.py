from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, List, Tuple


def load_wb_tokens(db_path: str | Path) -> List[Tuple[int, str]]:
    """Return organization IDs and Wildberries tokens from SQLite.

    Args:
        db_path: Path to ``finmodel.db``.

    Returns:
        List of ``(id, Token_WB)`` tuples. Rows with null or blank tokens are
        filtered out. Missing databases return an empty list.
    """

    p = Path(db_path)
    if not p.exists():
        return []

    query = "SELECT id, Token_WB FROM НастройкиОрганизаций WHERE Token_WB IS NOT NULL"
    with sqlite3.connect(str(p)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        rows: Iterable[sqlite3.Row]
        try:
            rows = cur.execute(query).fetchall()
        finally:
            cur.close()

    out: List[Tuple[int, str]] = []
    for r in rows:
        token = str(r["Token_WB"]).strip()
        if token:
            out.append((int(r["id"]), token))
    return out
