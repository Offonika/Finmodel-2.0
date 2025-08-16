#!/usr/bin/env bash
set -e

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python - <<'PY'
import sqlite3
from pathlib import Path

db = Path("finmodel.db")
if not db.exists():
    schema = Path("schema.sql").read_text()
    conn = sqlite3.connect(db)
    conn.executescript(schema)
    conn.close()
PY
