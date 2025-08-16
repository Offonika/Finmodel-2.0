#!/usr/bin/env pwsh
Set-StrictMode -Version Latest

python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

if (-Not (Test-Path "finmodel.db")) {
    python -c "import sqlite3, pathlib; conn = sqlite3.connect('finmodel.db'); conn.executescript(pathlib.Path('schema.sql').read_text()); conn.close()"
}
