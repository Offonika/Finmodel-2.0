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

## Docker
A Docker image can run any import script in an isolated environment.

Build the image:

```bash
docker build -t finmodel .
```

Run an import with your configuration file mounted into the container:

```bash
docker run --rm -v $(pwd)/config.yml:/app/config.yml finmodel
```

Override the script with the `FINMODEL_SCRIPT` variable:

```bash
docker run --rm -e FINMODEL_SCRIPT=finmodel.scripts.orderswb_import_flat \
  -v $(pwd)/config.yml:/app/config.yml finmodel
```

To use PostgreSQL instead of SQLite, start the application with `docker-compose`:

```yaml
version: "3.8"
services:
  app:
    build: .
    volumes:
      - ./config.yml:/app/config.yml:ro
    environment:
      - FINMODEL_SCRIPT=finmodel.scripts.saleswb_import_flat
    depends_on:
      - db
  db:
    image: postgres:15
    environment:
      POSTGRES_USER: finmodel
      POSTGRES_PASSWORD: finmodel
      POSTGRES_DB: finmodel
    volumes:
      - pgdata:/var/lib/postgresql/data
volumes:
  pgdata:
```

Launch the stack:

```bash
docker-compose up --build
```

## Scheduling
For regular data imports, schedule the container execution.

### Linux (cron)
Run `crontab -e` and add a line:

```
0 3 * * * docker run --rm -v /path/to/config.yml:/app/config.yml finmodel
```

This example runs the import every day at 03:00.

### Windows (Task Scheduler)
Create a basic task that runs:

```
docker run --rm -v C:\path\to\config.yml:/app/config.yml finmodel
```

Set the trigger according to the required interval.

## Development
- Follow instructions in `AGENTS.md` for coding standards and testing.
- Ensure new scripts include descriptive docstrings and a guarded `main` entry point.
- Run `python -m compileall -q .` to verify syntax before committing.

## Logs
All scripts emit logs to the `log/finmodel.log` file. Create the `log/` directory if it
does not exist to keep collected logs.

## License
Specify the project license if applicable.
