"""Command line interface for finmodel scripts."""

import pkgutil
from importlib import import_module
from pathlib import Path

import typer

app = typer.Typer(help="Finmodel command line interface")


def _run_module(module_name: str) -> None:
    module = import_module(f"finmodel.scripts.{module_name}")
    if not hasattr(module, "main"):
        typer.echo(f"Module {module_name} has no main() function.")
        raise typer.Exit(code=1)
    module.main()


def _create_command(name: str):
    def command() -> None:
        _run_module(name)

    command.__doc__ = f"Run {name} script."
    return command


scripts_dir = Path(__file__).resolve().parent / "scripts"
if scripts_dir.exists():
    for module_info in pkgutil.iter_modules([str(scripts_dir)]):
        app.command(module_info.name)(_create_command(module_info.name))


def _prompt_path(prompt: str, default: Path, must_exist: bool = False) -> Path:
    while True:
        user_input = typer.prompt(prompt, default=str(default))
        try:
            path = Path(user_input).expanduser()
        except Exception as exc:  # pragma: no cover - defensive
            typer.echo(f"Invalid path: {exc}")
            continue
        if must_exist and not path.exists():
            typer.echo("Path does not exist. Please try again.")
            continue
        if not must_exist and not path.parent.exists():
            typer.echo("Directory does not exist. Please try again.")
            continue
        return path


@app.command()
def menu() -> None:
    """Interactive menu to run common commands."""
    menu_text = (
        "=========== Finmodel 2.0 ===========\n"
        " 1. Import orders WB\n"
        " 2. Import sales WB\n"
        " 3. Import product catalog\n"
        " 4. Create new database from schema\n"
        " 5. Dump schema from current DB\n"
        " 0. Exit\n"
        "====================================="
    )
    while True:
        typer.echo(menu_text)
        choice = typer.prompt("Select an option").strip()
        try:
            if choice == "1":
                app.invoke(app.commands["orderswb_import_flat"].callback)
            elif choice == "2":
                app.invoke(app.commands["saleswb_import_flat"].callback)
            elif choice == "3":
                app.invoke(app.commands["katalog"].callback)
            elif choice == "4":
                db = _prompt_path("Database path", Path("finmodel.db"))
                schema = _prompt_path("Schema path", Path("schema.sql"), must_exist=True)
                from finmodel.scripts.create_db import main as create_db_main

                create_db_main(db=db, schema=schema)
            elif choice == "5":
                db = _prompt_path("Database path", Path("finmodel.db"), must_exist=True)
                output = _prompt_path("Output schema path", Path("schema.sql"))
                from finmodel.scripts.dump_schema import main as dump_schema_main

                dump_schema_main(db=db, output=output)
            elif choice == "0":
                break
            else:
                typer.echo("Invalid choice. Please try again.")
                continue
        except Exception as exc:  # pragma: no cover - defensive
            typer.echo(f"Error: {exc}")
        if not typer.confirm("Return to main menu?", default=True):
            break
    typer.echo("Goodbye!")


def main() -> None:
    """Entry point for console_scripts."""
    app()
