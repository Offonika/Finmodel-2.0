from __future__ import annotations

import sqlite3
from pathlib import Path

import typer

from finmodel.logger import setup_logging


def create_db(db_path: Path, schema_path: Path) -> None:
    """Create an SQLite database using the provided schema."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    schema_sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_sql)


def main(
    db: Path = typer.Option(Path("finmodel.db"), help="Path to SQLite database to create."),
    schema: Path = typer.Option(Path("schema.sql"), help="Path to SQL schema file."),
) -> None:
    """Create an SQLite database from a schema file."""
    setup_logging()
    create_db(db, schema)
    typer.echo(f"Database created at {db}")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    typer.run(main)
