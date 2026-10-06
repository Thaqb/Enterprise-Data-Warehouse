FROM apache/airflow:3.1.3

# Install Cosmos and dbt dependencies for Airflow orchestration
# - astronomer-cosmos: Run dbt Core projects as Airflow DAGs/TaskGroups
# - dbt-bigquery: dbt adapter for BigQuery (needed for Cosmos to parse dbt project)
# - apache-airflow-providers-google: Airflow BigQuery connection support

USER root

# Install any system dependencies if needed
# RUN apt-get update && apt-get install -y <package> && apt-get clean

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.11.0 \
    dbt-bigquery==1.11.0 \
    apache-airflow-providers-google==10.24.0 \
    "dlt[bigquery,duckdb]>=1.20.0" \
    "requests>=2.31.0"

# uv builds the ingestion_layer venv inside the container (`uv sync`).
# only-system: never download a Python, always link the venv to the image's own
# interpreter, otherwise the venv breaks when the container is recreated.
RUN pip install --no-cache-dir uv
ENV UV_PYTHON_PREFERENCE=only-system
