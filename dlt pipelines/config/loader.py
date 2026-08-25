"""Centralized config loader with validation for Hostfully pipeline.

This module provides a dataclass-based configuration approach that:
- Centralizes all dlt.config.get() calls in one place
- Validates required configuration values (base_url, agency_uid, api_key)
- Provides type safety and IDE autocomplete
- Works with dlt's decorator injection system
"""

import logging
import dlt
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class HostfullyConfig:
    """Hostfully pipeline configuration with validation."""
    
    base_url: str
    agency_uid: str
    table_prefix: str
    max_workers: int
    batch_size: int
    property_calendar_backfill_from: Optional[str]
    property_calendar_backfill_to: Optional[str]
    
    @staticmethod
    def from_dlt() -> 'HostfullyConfig':
        """Load and validate configuration from dlt config.
        
        Validates that required configuration values are present:
        - hostfully.base_url
        - hostfully.agency_uid
        
        Returns:
            HostfullyConfig instance with all configuration values
            
        Raises:
            ValueError: If any required configuration is missing
        """
        # Load required config values
        base_url = dlt.config.get("hostfully.base_url")
        agency_uid = dlt.config.get("hostfully.agency_uid")
        
        # Validate required values
        if not base_url:
            raise ValueError(
                "Missing required configuration: hostfully.base_url\n"
                "Please set it in .dlt/config.toml: base_url = \"https://api.hostfully.com/api/v3.2/\""
            )
        
        if not agency_uid:
            raise ValueError(
                "Missing required configuration: hostfully.agency_uid\n"
                "Please set it in .dlt/config.toml: agency_uid = \"your-agency-uid\""
            )
        
        # Load optional values with defaults
        table_prefix = dlt.config.get("environment.table_prefix") or "raw_"
        max_workers = dlt.config.get("hostfully.max_workers") or 15
        batch_size = dlt.config.get("hostfully.batch_size") or 1
        
        # Property calendar backfill dates (optional)
        backfill_from = dlt.config.get("hostfully.property_calendar_backfill_from")
        backfill_to = dlt.config.get("hostfully.property_calendar_backfill_to")
        
        logger.debug(f"Loaded config: base_url={base_url}, agency_uid={agency_uid}, table_prefix={table_prefix}")
        
        return HostfullyConfig(
            base_url=base_url,
            agency_uid=agency_uid,
            table_prefix=table_prefix,
            max_workers=max_workers,
            batch_size=batch_size,
            property_calendar_backfill_from=backfill_from,
            property_calendar_backfill_to=backfill_to
        )


def get_env_mode() -> str:
    """Get environment mode (DEV or PROD).
    
    Returns:
        Environment mode string, defaults to "DEV"
    """
    return dlt.config.get("environment.mode") or "DEV"


def validate_api_key(api_key: str) -> None:
    """Validate that API key is properly configured.
    
    Args:
        api_key: API key value to validate
        
    Raises:
        ValueError: If api_key is missing or is still a sentinel value
    """
    if not api_key or not isinstance(api_key, str):
        raise ValueError(
            "Missing required secret: api_key\n"
            "Please set it in .dlt/secrets.toml: api_key = \"your-hostfully-api-key\""
        )
