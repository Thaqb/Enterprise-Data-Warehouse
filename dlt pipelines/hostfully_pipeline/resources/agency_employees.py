"""Lead resources for Hostfully API."""

import logging
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from config import HostfullyConfig

logger = logging.getLogger(__name__)


@dlt.source(name="hostfully",parallelized=True)
def hostfully_rest_api_source(
    api_key: str = dlt.secrets.value
):
    """Define dlt REST API resources for Hostfully agencies, employees, and owners.

    This source yields three REST resources:
      - `agencies`: single-page endpoint
      - `employees`: single-page endpoint filtered by `agencyUid`
      - `owners`: single-page endpoint with `max_table_nesting=0`

    Args:
        api_key: Hostfully API key (read from .dlt/secrets.toml)

    Yields:
        A dlt REST API resources generator for `agencies`, `employees`, and `owners`.
    """
    # Load config inside function
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
                "name": "agencies",
                "write_disposition": "merge",
                "primary_key": "uid",
                "table_name": f"{config.table_prefix}agency",
                "endpoint": {
                    "path": "agencies",
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "agencies"
                }
            },
            {
                "name": "employees",
                "write_disposition": "merge",
                "primary_key": "uid",
                "table_name": f"{config.table_prefix}employees",
                "endpoint": {
                    "path": "employees",
                    "params": {
                        "agencyUid": config.agency_uid
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "employees"
                }
            },
            {
                "name":"promo_codes",
                "write_disposition":"merge",
                "primary_key":"uid",
                "table_name": f"{config.table_prefix}promo_codes",
                "max_table_nesting":0,
                "endpoint":{
                    "path":"promo-codes",
                    "params":{
                        "_limit":1000
                    },
                    "paginator": {
                        "type": "cursor",
                        "cursor_param": "_cursor",
                        "cursor_path": "_paging._nextCursor"
                    },
                    "data_selector": "promoCodes"
                }
            }
        ]
    }
    yield from rest_api_resources(rest_config)

