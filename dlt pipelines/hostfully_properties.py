"""Entrypoint to run the Properties-only daily pipeline."""

import logging
import dlt
from hostfully_pipeline.resources.properties import hostfully_properties_source
from hostfully_pipeline.resources.property_calendar import property_calendar_transformer_factory
from hostfully_pipeline.resources.property_reviews_airbnb import property_reviews_airbnb_factory
from hostfully_pipeline.resources.property_reviews_booking import property_reviews_booking_factory
from hostfully_pipeline.utils import property_uids_from_db as property_uids_from_db_resource
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
        dataset_name=env_dataset
    )

# =============================================================================
# STAGE 1: PROPERTIES (must run first to populate raw_properties table)
# =============================================================================
logger.info("=" * 80)
logger.info("=== STAGE 1: PROPERTIES ===")
logger.info("=" * 80)

properties_source = hostfully_properties_source()
info1 = pipeline.run(properties_source)
logger.info(f'[STAGE 1] Properties load completed.\n{info1}')

# =============================================================================
# STAGE 2: CALENDAR + REVIEWS (depends on property UIDs from raw_properties)
# =============================================================================
logger.info("=" * 80)
logger.info("=== STAGE 2: CALENDAR + REVIEWS ===")
logger.info("=" * 80)

# Get property UIDs from the now-populated raw_properties table
uids_resource = property_uids_from_db_resource(pipeline_name=pipeline.pipeline_name)

# Add transformed resources to the source to keep under same 'hostfully' schema
properties_source.resources.add(uids_resource | property_calendar_transformer_factory())
properties_source.resources.add(uids_resource | property_reviews_airbnb_factory())
properties_source.resources.add(uids_resource | property_reviews_booking_factory())

# Run only the transformer resources (properties already loaded in Stage 1)
info2 = pipeline.run([
    properties_source.resources["property_calendar"],
    properties_source.resources["property_reviews_airbnb"],
    properties_source.resources["property_reviews_booking"]
])
logger.info(f'[STAGE 2] Calendar and Reviews load completed.\n{info2}')

logger.info("=" * 80)
logger.info("Properties pipeline completed successfully!")
logger.info("=" * 80)