# Finmodel 2.0

Python utilities for importing and analyzing financial data from the Wildberries marketplace.

## Features
- Import advertising campaigns, orders, stocks, tariffs and more using dedicated scripts.
- Store imported records in `finmodel.db` for subsequent analysis.

## Quick start
1. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt  # if available
   ```
2. Configure any required API credentials for Wildberries in environment variables.
3. Run a script, e.g.:
   ```bash
   python saleswb_import_flat.py
   ```

## Development
- Follow instructions in `AGENTS.md` for coding standards and testing.
- Ensure new scripts include descriptive docstrings and a guarded `main` entry point.
- Run `python -m compileall -q .` to verify syntax before committing.

## License
Specify the project license if applicable.
