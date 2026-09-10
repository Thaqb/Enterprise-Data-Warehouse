"""
Hostfully Leads Pipeline DAG

Schedule: Hourly
Pipeline: dlt extraction (leads, orders, transactions, messages) → dbt transformation (hourly staging + marts)
Email: mahmoudmostafa@partment.co (on failure)

This DAG orchestrates:
1. Extract data from Hostfully API using dlt (hostfully_leads.py)
2. Transform data using dbt (tag:hourly+ includes staging and dependent marts)
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
from airflow.sdk import dag ,TaskGroup,task
from airflow.providers.standard.operators.bash import BashOperator
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
    dag_id="hostfully_leads_pipeline",
    default_args=default_args,
    description="Hourly: dlt leads extraction → dbt hourly transformations",
    schedule="@hourly",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["hostfully", "hourly", "leads", "dlt", "dbt"],
)

def hostfully_leads_dag():
    """
    Hostfully Leads Pipeline DAG
    
    Runs hourly to keep booking/order/transaction data fresh.
    """
    
    # Task 1: Extract data using dlt (dlt is installed in Airflow container)
    # dlt_extract_leads = BashOperator(
    #     task_id="dlt_extract_leads",
    #     bash_command=f"""
    #     set -e
    #     cd {DLT_PROJECT_DIR}
    #     python hostfully_leads.py
    #     """,
    #     env={
    #         "DLT_PROJECT_DIR": DLT_PROJECT_DIR,
    #     },
    # )
    @task(task_id="dlt_extract_leads")
    def get_leads():
        sys.path.append('/opt/airflow/ingestion_layer')
        from hostfully_leads import run_leads_pipeline
        run_leads_pipeline()
    dlt_extract_leads = get_leads()
    # Task 2: Transform data using dbt (hourly staging models + downstream marts)
    dbt_transform_hourly = DbtTaskGroup(
        group_id="dbt_transform_hourly",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
            manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:hourly+"],  # Select hourly staging + all downstream models
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
    # dlt_extract_leads >> dbt_transform_hourly
    dlt_extract_leads >> dbt_transform_hourly


# Instantiate the DAG
hostfully_leads_dag()
