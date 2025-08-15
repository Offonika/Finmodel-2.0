import argparse
import sqlite3
from pathlib import Path


def create_database(db_path: Path, schema_path: Path) -> None:
    schema = schema_path.read_text(encoding="utf-8")
    conn = sqlite3.connect(db_path)
    conn.executescript(schema)
    conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create SQLite database from a schema file")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("finmodel.db"),
        help="Path to the database file to create",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path("schema.sql"),
        help="Path to the SQL schema file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    create_database(args.db, args.schema)
    print(f"Database created at {args.db}")


if __name__ == "__main__":
    main()
