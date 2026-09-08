"""Entrypoint to run the Properties-only daily pipeline."""

import logging
import dlt
from config.loader import HostfullyConfig
from config.conf_pipeline import PipelineConfig
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


hostfully_config = HostfullyConfig.from_dlt()
pipeline_config = PipelineConfig.from_environment()

logger.info(f"Pipeline running in {pipeline_config.environment} mode (destination: {pipeline_config.destination}, dataset: {pipeline_config.dataset})")

pipeline = dlt.pipeline(
    pipeline_name="hostfully_pipeline",
    destination=pipeline_config.destination,
    dataset_name=pipeline_config.dataset
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
uids_resource = property_uids_from_db_resource(
    pipeline_name=pipeline.pipeline_name,
    pipeline_config=pipeline_config
)

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