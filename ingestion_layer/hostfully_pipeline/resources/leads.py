"""Lead resources for Hostfully API."""

import logging
import dlt
from typing import Iterator
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from config import HostfullyConfig

logger = logging.getLogger(__name__)


@dlt.source(name='hostfully',parallelized=True)
def hostfully_rest_api_source(
    api_key: str = dlt.secrets.value
):
    """Define dlt resources from Hostfully REST API endpoints.
    
    Args:
        api_key: Hostfully API key (read from .dlt/secrets.toml)
        
    Yields:
        REST API resources for Hostfully endpoints
    """
    # Load config inside function to avoid mutable default parameter issues
    config = HostfullyConfig.from_dlt()
    
    rest_config: RESTAPIConfig = {
        "client": {
            "base_url": config.base_url,
            "headers": {
                "X-HOSTFULLY-APIKEY": api_key
            }
        },
        "resources": [
            {
                "name": "leads",
                "write_disposition": "merge",
                "primary_key": "uid",
                "table_name": "leads",
                "endpoint": {
                    "path": "leads",
                    "params": {
                        "_limit": 1000,
                        "agencyUid": config.agency_uid,
                        "updatedSince": "{incremental.start_value}"
                    },
                    "paginator": {
                        "type": "cursor",
                        "cursor_param": "_cursor",
                        "cursor_path": "_paging._nextCursor"
                    },
                    "data_selector": "leads",
                    "incremental": {
                        "cursor_path": "metadata.updatedUtcDateTime",
                        "initial_value": "2020-01-01T00:00:00"
                    }
                }
            }
        ]
    }
    yield from rest_api_resources(rest_config)
