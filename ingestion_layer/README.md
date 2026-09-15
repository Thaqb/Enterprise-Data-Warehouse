# Ingestion Layer — Environment Configuration
## Installation and Setup
This project uses **uv** for Python dependency and environment management.

### Requirements

* `Python 3.12.3`
* uv >=`0.1.0` 

The exact Python version used by the project is defined in `.python-version`.

### Install dependencies

From the `ingestion_layer` directory, run:

```bash
uv sync
```

`uv sync` creates the project's virtual environment and installs the dependencies defined in `pyproject.toml` and locked in `uv.lock`.

The project's main dependencies are:

| Package       | Version    |
| ------------- | ---------- |
| `dlt[duckdb]` | `>=1.20.0` |
| `requests`    | `>=2.31.0` |

### Verify the Python version

To check the Python version used by the project:

```bash
uv run python --version
```

The output should match the version defined in `.python-version`.

### View installed packages

To list the packages installed in the project's environment:

```bash
uv pip list
```

This displays the packages installed in the active `.venv`, including their resolved versions.

You can also use:

```bash
uv pip freeze
```

to display the installed packages in requirements-style format.

> `uv pip list` shows the packages currently installed in the environment. The dependency source of truth for this project is `pyproject.toml`, while `uv.lock` records the exact resolved dependency versions.

## Pipeline Config Overview

The Hostfully dlt pipelines run from the same codebase in both **development** and **production**. The active environment is selected entirely through the `APP_ENV` environment variable — no environment-selection logic lives in Python source.

Supported values (case-sensitive, lowercase only):

| `APP_ENV` | Destination | Dataset          |
| --------- | ----------- | ---------------- |
| `dev`     | `duckdb`    | `dev_hostfully`  |
| `prod`    | `bigquery`  | `prod_hostfully` |

Any other value — missing, empty, `DEV`, `PROD`, `test`, etc. — causes the pipeline to raise `ValueError` immediately. There is no fallback to `dev`.

## Running locally (dev)

Set `APP_ENV` to `dev` before running the pipeline.

**Windows (PowerShell, persists across terminals):**

```powershell
[System.Environment]::SetEnvironmentVariable("APP_ENV", "dev", "Machine")
```

Open a new terminal, then verify:

```powershell
$env:APP_ENV
```

**Linux:**

Add to `/etc/environment`:

```text
APP_ENV=dev
```

Start a new login session, then verify:

```bash
echo $APP_ENV
```

After setting up the environment and installing dependencies, run the pipeline with:

```bash
uv run python hostfully_leads.py
```

Using `uv run` ensures that the command runs with the project's managed environment and dependencies.

## Running in production (Airflow)

Production runs non-interactively inside Airflow's Docker containers. `APP_ENV=prod` is set in `airflow-prod-compose/.env`, which is loaded via `env_file` for every Airflow service in `docker-compose.yaml` — no per-DAG or per-task environment wiring is required; tasks inherit it from the container.

## Notes

* `APP_ENV` is only ever read in `config/conf_pipeline.py`. It must not be set in `.dlt/config.toml` or in any pipeline module.
* dbt's `target_name="prod"` in the Airflow DAGs is a separate, intentional setting unrelated to `APP_ENV`/`PipelineConfig` — do not connect the two.
* Dependencies are managed through `pyproject.toml` and `uv.lock`. Do not use `requirements.txt` for dependency installation.
