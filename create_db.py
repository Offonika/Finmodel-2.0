#!/usr/bin/env python3
"""CLI utility to create SQLite database from SQL schema."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def create_db(db_path: Path, schema_path: Path) -> None:
    """Create an SQLite database and populate it using the provided schema."""

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")


    schema_sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_sql)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create SQLite database from an SQL schema file.")

    parser.add_argument(
        "db",
        type=Path,
        nargs="?",
        default=Path("finmodel.db"),
        help="Path to the SQLite database to create (default: finmodel.db).",
    )
    parser.add_argument(
        "schema",
        type=Path,
        nargs="?",
        default=Path("schema.sql"),
        help="Path to the SQL schema file (default: schema.sql).",
    )

    args = parser.parse_args()

    create_db(args.db, args.schema)
    print(f"Database created at {args.db}")


if __name__ == "__main__":
    main()
