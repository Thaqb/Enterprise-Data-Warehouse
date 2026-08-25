"""Entrypoint to run the Employees,agency and owners monthly pipeline."""

import logging
import dlt
from hostfully_pipeline.resources.agency_employees import hostfully_rest_api_source
# logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Read environment mode from config (DEV or PROD)
try:
    env_mode = dlt.config.get("environment.mode") or "DEV"
except Exception:
    env_mode = "DEV"
env_destination = "bigquery" if env_mode == "PROD" else "duckdb"
env_dataset = f"{env_mode.lower()}_hostfully"
logger.info(f"Pipeline running in {env_mode} mode (destination: {env_destination}, dataset: {env_dataset})")


pipeline = dlt.pipeline(
        pipeline_name="hostfully_pipeline",
        destination=env_destination,
        dataset_name=env_dataset,
        progress='log'
    )
source = hostfully_rest_api_source()

info1 = pipeline.run(source)
logger.info(f'Employees resource load completed.\n{info1}')