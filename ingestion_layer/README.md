# Ingestion Layer — Environment Configuration

## Overview

The Hostfully dlt pipelines run from the same codebase in both **development** and **production**. The active environment is selected entirely through the `APP_ENV` environment variable — no environment-selection logic lives in Python source.

Supported values (case-sensitive, lowercase only):

| `APP_ENV` | Destination | Dataset          |
| --------- | ----------- | ---------------- |
| `dev`     | `duckdb`    | `dev_hostfully`  |
| `prod`    | `bigquery`  | `prod_hostfully` |

Any other value — missing, empty, `DEV`, `PROD`, `test`, etc. — causes the pipeline to raise `ValueError` immediately. There is no fallback to `dev`.

## Running locally (dev)

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

```
APP_ENV=dev
```

Start a new login session, then verify:

```bash
echo $APP_ENV
```

Then run any entry point directly, e.g.:

```bash
python hostfully_leads.py
```

## Running in production (Airflow)

Production runs non-interactively inside Airflow's Docker containers. `APP_ENV=prod` is set in `airflow-prod-compose/.env`, which is loaded via `env_file` for every Airflow service in `docker-compose.yaml` — no per-DAG or per-task environment wiring is required; tasks inherit it from the container.

## Notes

- `APP_ENV` is only ever read in `config/conf_pipeline.py`. It must not be set in `.dlt/config.toml` or in any pipeline module.
- dbt's `target_name="prod"` in the Airflow DAGs is a separate, intentional setting unrelated to `APP_ENV`/`PipelineConfig` — do not connect the two.
