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


@app.command()
def menu() -> None:
    """Interactive menu to run common commands."""
    command_names = sorted(name for name in app.registered_commands if name != "menu")
    if not command_names:
        typer.echo("No commands available.")
        return

    while True:
        typer.echo("=========== Finmodel 2.0 ===========")
        for idx, name in enumerate(command_names, start=1):
            typer.echo(f" {idx}. {name}")
        typer.echo(" 0. Exit")
        typer.echo("====================================")
        choice = typer.prompt("Select an option").strip()
        if choice == "0":
            break
        try:
            command_name = command_names[int(choice) - 1]
        except (ValueError, IndexError):
            typer.echo("Invalid choice. Please try again.")
            continue
        try:
            app.registered_commands[command_name].callback()
        except Exception as exc:  # pragma: no cover - defensive
            typer.echo(f"Error: {exc}")
        if not typer.confirm("Return to main menu?", default=True):
            break
    typer.echo("Goodbye!")


def main() -> None:
    """Entry point for console_scripts."""
    app()
