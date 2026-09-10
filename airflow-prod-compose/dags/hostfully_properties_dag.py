"""
Hostfully Properties Pipeline DAG

Schedule: Daily at 7 AM UTC
Pipeline: dlt extraction (properties, calendar, reviews) → dbt transformation (daily staging + marts) → dbt snapshots
Email: mahmoudmostafa@partment.co (on failure)

This DAG orchestrates:
1. Extract data from Hostfully API using dlt (hostfully_properties.py)
2. Transform data using dbt (tag:daily+ includes staging and dependent marts)
3. Run dbt snapshots (snp_fct_bookings, snp_fct_pricing, snp_dim_properties, snp_hostfully__promo_codes)
"""

from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, TestBehavior, LoadMode

# Path configurations
DLT_PROJECT_DIR = os.getenv("DLT_PROJECT_DIR", "/opt/airflow/ingestion_layer")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
GCP_CREDENTIALS_PATH = "/opt/airflow/secrets/gcp_credentials.json"

# Email configuration
NOTIFICATION_EMAIL = "mahmoudmostafa@partment.co"

# Default arguments for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [NOTIFICATION_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# dbt profile configuration - use profiles.yml directly from dbt project
profile_config = ProfileConfig(
    profile_name="hostfully",
    target_name="prod",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

# dbt execution configuration
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)


@dag(
    dag_id="hostfully_properties_pipeline",
    default_args=default_args,
    description="Daily: dlt properties extraction → dbt daily transformations → snapshots",
    schedule="10 7 * * *",  # Daily at 7:10 AM UTC
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["hostfully", "daily", "properties", "dlt", "dbt", "snapshots"],
)
def hostfully_properties_dag():
    """
    Hostfully Properties Pipeline DAG
    
    Runs daily at 7 AM UTC to sync property data, calendar, and reviews.
    Also runs dbt snapshots to capture slowly changing dimensions.
    """
    
    # Task 1: Extract data using dlt (dlt is installed in Airflow container)
    dlt_extract_properties = BashOperator(
        task_id="dlt_extract_properties",
        bash_command=f"""
        set -e
        cd {DLT_PROJECT_DIR}
        python hostfully_properties.py
        """,
        env={
            "DLT_PROJECT_DIR": DLT_PROJECT_DIR,
        },
    )
    
    # Task 2: Transform data using dbt (daily staging models + downstream marts)
    dbt_transform_daily = DbtTaskGroup(
        group_id="dbt_transform_daily",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:daily+"],  # Select daily staging + all downstream models
            test_behavior=TestBehavior.AFTER_EACH,  # Run tests after each model
            load_method=LoadMode.DBT_MANIFEST,  # Use manifest for faster parsing
            env_vars={"GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDENTIALS_PATH},
        ),
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=3),
        },
    )
    
    # Task 3: Run dbt snapshots (dbt is installed in Airflow container)
    # Snapshots: snp_fct_bookings, snp_fct_pricing, snp_dim_properties, snp_hostfully__promo_codes
    # dbt_run_snapshots = BashOperator(
    #     task_id="dbt_run_snapshots",
    #     bash_command=f"""
    #     set -e
    #     cd {DBT_PROJECT_DIR}
    #     dbt snapshot --profiles-dir . --target prod
    #     """,
    #     env={
    #         "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    #         "GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDENTIALS_PATH,
    #     },
    # )
    
    # Define task dependencies
    dlt_extract_properties >> dbt_transform_daily


# Instantiate the DAG
hostfully_properties_dag()
