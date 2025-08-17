from __future__ import annotations

import sqlite3
from pathlib import Path

import typer

from finmodel.logger import setup_logging


def dump_schema(db_path: Path, output: Path) -> None:
    """Dump SQL schema from the given database into a file."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type IN ('table', 'index', 'trigger')
            AND name NOT LIKE 'sqlite_%'
            """
        )
        schema = [row[0] for row in cursor.fetchall() if row[0]]
    with output.open("w", encoding="utf-8") as f:
        for stmt in schema:
            f.write(stmt.strip() + ";\n\n")


def main(
    db: Path = typer.Option(Path("finmodel.db"), help="Path to SQLite database."),
    output: Path = typer.Option(Path("schema.sql"), help="Path for output schema file."),
) -> None:
    """Dump SQL schema from an SQLite database."""
    setup_logging()
    dump_schema(db, output)
    typer.echo(f"Schema dumped to {output}")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    typer.run(main)
