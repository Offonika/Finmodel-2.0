# AGENTS Instructions

These guidelines apply to the entire repository.

## Environment
- Target Python 3.10 or newer.
- Work inside a virtual environment and install dependencies from `requirements.txt` when available.

## Workflow
- Format code with `black` (line length 100) and sort imports with `isort`.
- Run `python -m compileall -q .` before committing. If tests exist, also run `pytest` and ensure all pass.
- Write commit messages in English using the imperative mood.

## Style
- Follow PEP 8 with 4-space indentation.
- Use descriptive names and keep code and comments in English.
- Avoid executing logic at import time; use `if __name__ == "__main__":` for script entry points.

## Documentation
- Update `README.md` whenever new scripts or workflows are added or modified.
