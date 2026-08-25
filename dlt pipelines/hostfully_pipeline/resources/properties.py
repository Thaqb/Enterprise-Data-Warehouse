"""Properties resource for Hostfully API."""

import logging
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from config import HostfullyConfig

logger = logging.getLogger(__name__)


@dlt.source(name='hostfully',parallelized=True)
def hostfully_properties_source(
    api_key: str = dlt.secrets.value
):
    """Define dlt resource for Hostfully properties endpoint.

    Uses `rest_api_resources` to leverage dlt's built-in paging, rate-limit handling,
    and incremental helpers when applicable. The `properties` table uses `uid`
    as the primary key and `merge` write disposition so runs are idempotent.

    Args:
        api_key: Hostfully API key (read from .dlt/secrets.toml)

    Yields:
        REST API resources for the properties endpoint
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
                "name": "properties",
                "write_disposition": "merge",
                "primary_key": "uid",
                "table_name": f"{config.table_prefix}properties",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "properties",
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
                    "data_selector": "properties",
                    "incremental": {
                        "cursor_path": "updatedUtcDateTime",
                        "initial_value": "2020-01-01T00:00:00"
                    }
                }
            },
            {
                "name": "property-descriptions",
                "write_disposition": "merge",
                "primary_key":"_properties_uid",
                "table_name": f"{config.table_prefix}property_descriptions",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "property-descriptions",
                    "params": {
                        "propertyUid":"{resources.properties.uid}"
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "propertyDescriptions"
                },
                "include_from_parent": ["uid"],
                "processing_steps": 
                [
                    {"filter": lambda x: x["locale"] != "en-US" }
                ]
            },
            {
                "name": "property-channel-links",
                "write_disposition": "merge",
                "primary_key":"propertyUid",
                "table_name": f"{config.table_prefix}property_channel_links",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "property-channel-links",
                    "params": {
                        "propertyUid":"{resources.properties.uid}"
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "propertyChannelLinks"
                }
            },
            {
                "name": "property-ownership",
                "write_disposition": "merge",
                "primary_key":"propertyUid",
                "table_name": f"{config.table_prefix}property_owner",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "property-ownership/{resources.properties.uid}",
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "propertyOwnership"
                }
            },
            {
                "name": "available-property-rules",
                "write_disposition": "merge",
                "primary_key":"_properties_uid",
                "table_name": f"{config.table_prefix}property_rules",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "available-property-rules",
                    "params":{
                        "propertyUid":"{resources.properties.uid}"
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "availablePropertyRules"
                },
                "include_from_parent": ["uid"]
            },
            {
                "name": "property-amenities",
                "write_disposition": "merge",
                "primary_key":["uid","propertyUid"],
                "table_name": f"{config.table_prefix}property_amenities",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "amenities",
                    "params":{
                        "propertyUid":"{resources.properties.uid}"
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "amenities"
                }
            },
            {
                "name": "property-booking-settings",
                "write_disposition": "merge",
                "primary_key":"_properties_uid",
                "table_name": f"{config.table_prefix}property_booking_dot_com_settings",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "https://platform.hostfully.com/api/internal/properties/{resources.properties.uid}/main-settings",
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "bookingDotComSettings"
                },
                "include_from_parent": ["uid"],
                "processing_steps": 
                [
                    {"filter": lambda x: x["hotelId"] is not None}
                ]
            },
            {
                "name": "property-booking-status",
                "write_disposition": "merge",
                "primary_key":"uid",
                "table_name": f"{config.table_prefix}booking_dot_com_status",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "https://platform.hostfully.com/api/internal/bookingdotcom-operations/property-status",
                    "params":{
                        "uid":"{resources.properties.uid}"
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "response_actions": [
                        {"status_code": 400, "action": "ignore"}
                    ]
                }
            },
            {
                "name":"owners",
                "write_disposition":"merge",
                "primary_key":"uid",
                "table_name": f"{config.table_prefix}owners",
                "max_table_nesting":0,
                "endpoint":{
                    "path":"owners",
                    "params":{
                        "agencyUid":config.agency_uid
                    },
                    "paginator": {
                        "type": "single_page"
                    },
                    "data_selector": "owners"
                }
            }
        ]
    }
    yield from rest_api_resources(rest_config)
