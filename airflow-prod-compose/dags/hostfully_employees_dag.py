"""
Hostfully Employees Pipeline DAG

Schedule: Biweekly (1st and 15th of each month at midnight UTC)
Pipeline: dlt extraction (agencies, employees, owners, promo_codes) → dbt transformation (biweekly staging + marts)
Email: mahmoudmostafa@partment.co (on failure)

This DAG orchestrates:
1. Extract data from Hostfully API using dlt (hostfully_empolyees.py)
2. Transform data using dbt (tag:biweekly+ includes staging and dependent marts)
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
    dag_id="hostfully_employees_pipeline",
    default_args=default_args,
    description="Biweekly: dlt employees extraction → dbt biweekly transformations",
    schedule="0 0 1,15 * *",  # At 00:00 on day 1 and 15 of every month
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["hostfully", "biweekly", "employees", "dlt", "dbt"],
)
def hostfully_employees_dag():
    """
    Hostfully Employees Pipeline DAG
    
    Runs biweekly (1st and 15th) to sync organizational data (agencies, employees, owners, promo codes).
    """
    
    # Task 1: Extract data using dlt (dlt is installed in Airflow container)
    dlt_extract_employees = BashOperator(
        task_id="dlt_extract_employees",
        bash_command=f"""
        set -e
        cd {DLT_PROJECT_DIR}
        python hostfully_empolyees.py
        """,
        env={
            "DLT_PROJECT_DIR": DLT_PROJECT_DIR,
        },
    )
    
    # Task 2: Transform data using dbt (biweekly staging models + downstream marts)
    dbt_transform_biweekly = DbtTaskGroup(
        group_id="dbt_transform_biweekly",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:biweekly+"],  # Select biweekly staging + all downstream models
            test_behavior=TestBehavior.AFTER_EACH,  # Run tests after each model
            load_method=LoadMode.DBT_MANIFEST,  # Use manifest for faster parsing
            env_vars={"GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDENTIALS_PATH},
        ),
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=3),
        },
    )
    
    # Define task dependencies
    dlt_extract_employees >> dbt_transform_biweekly


# Instantiate the DAG
hostfully_employees_dag()
