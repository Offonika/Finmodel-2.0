from __future__ import annotations


from pathlib import Path
from typing import List, Tuple

from finmodel.utils.settings import load_organizations


def load_wb_tokens(
    path: str | Path | None = None, sheet: str | None = None
) -> List[Tuple[int, str]]:
    """Return organization IDs and Wildberries tokens from ``Настройки.xlsm``.

    Args:
        path: Optional path to the workbook. Defaults to ``Настройки.xlsm``
            in the project root.
        sheet: Optional sheet name. Defaults to the value resolved inside
            :func:`load_organizations` (``"НастройкиОрганизаций"``).

    Returns:
        List of ``(id, Token_WB)`` tuples. Rows with blank tokens are filtered
        out. Missing or malformed workbooks return an empty list.
    """

    df = load_organizations(path=path, sheet=sheet)
    tokens: List[Tuple[int, str]] = []
    if not df.empty:
        for _, row in df.iterrows():
            token = str(row.get("Token_WB", "")).strip()
            if token:
                tokens.append((int(row["id"]), token))
    return tokens
