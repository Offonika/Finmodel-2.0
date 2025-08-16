#!/usr/bin/env python3
"""CLI utility to create SQLite database from SQL schema."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def create_db(db_path: Path, schema_path: Path) -> None:
    """Create an SQLite database and populate it using the provided schema."""
    schema_sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_sql)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create SQLite database from an SQL schema file.")
    parser.add_argument("db", type=Path, help="Path to the SQLite database to create.")
    parser.add_argument("schema", type=Path, help="Path to the SQL schema file.")
    args = parser.parse_args()

    create_db(args.db, args.schema)
    print(f"Database created at {args.db}")


if __name__ == "__main__":
    main()
