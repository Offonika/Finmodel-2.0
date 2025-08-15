# Finmodel 2.0

Python utilities for importing and analyzing financial data from the Wildberries marketplace.

## Features
- Import advertising campaigns, orders, stocks, tariffs and more using dedicated scripts.
- Store imported records in `finmodel.db` for subsequent analysis.

## Installation
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `config.example.yml` to `config.yml` and fill in database path, date ranges
   and tokens. Environment variables with the same keys override the file values.
4. Run a script via the package module or installed console entry point, e.g.:
   ```bash
   python -m finmodel.scripts.saleswb_import_flat
   # or after installing the package:
   saleswb_import_flat
   ```

## Development
- Follow instructions in `AGENTS.md` for coding standards and testing.
- Ensure new scripts include descriptive docstrings and a guarded `main` entry point.
- Run `python -m compileall -q .` to verify syntax before committing.

## Logs
All scripts emit logs to the `log/finmodel.log` file. Create the `log/` directory if it
does not exist to keep collected logs.

## License
Specify the project license if applicable.
