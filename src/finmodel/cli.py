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


def main() -> None:
    """Entry point for console_scripts."""
    app()
