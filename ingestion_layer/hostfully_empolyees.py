"""Entrypoint to run the Employees,agency and owners monthly pipeline."""

import logging
import dlt
from config.loader import HostfullyConfig
from config.conf_pipeline import PipelineConfig
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


hostfully_config = HostfullyConfig.from_dlt()
pipeline_config = PipelineConfig.from_environment()

logger.info(f"Pipeline running in {pipeline_config.environment} mode (destination: {pipeline_config.destination}, dataset: {pipeline_config.dataset})")


pipeline = dlt.pipeline(
        pipeline_name="hostfully_pipeline",
        destination=pipeline_config.destination,
        dataset_name=pipeline_config.dataset,
        progress='log'
    )
source = hostfully_rest_api_source()

info1 = pipeline.run(source)
logger.info(f'Employees resource load completed.\n{info1}')