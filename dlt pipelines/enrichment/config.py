"""Configuration for enrichment rules.

Defines the enrichment rules for each endpoint, specifying which nested fields
require enrichment and how to fetch the complete data.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class EnrichmentRule:
    """Configuration for enriching a single endpoint with incomplete nested data.
    
    Attributes:
        endpoint_name: Name of the resource (e.g., 'leads')
        detail_endpoint_path: API path for detail endpoint (e.g., 'leads/{uid}')
        detail_response_key: Key in detail response containing the complete object (e.g., 'lead')
        uid_field: Primary key field name (e.g., 'uid')
        nested_field_name: Field name that may be incomplete (e.g., 'assignee')
        nested_uid_field: The sub-field within nested object that should be non-null (e.g., 'uid')
        retry_attempts: Number of retry attempts for failed detail fetches (default: 3)
        retry_delay_seconds: Delay between retries in seconds (default: 2)
    """
    endpoint_name: str
    detail_endpoint_path: str
    detail_response_key: str
    uid_field: str = "uid"
    nested_field_name: str = "assignee"
    nested_uid_field: str = "uid"
    retry_attempts: int = 3
    retry_delay_seconds: float = 0.5


@dataclass
class EnrichmentConfig:
    """Container for all enrichment rules.
    
    Attributes:
        rules: Dict mapping endpoint names to their enrichment rules
    """
    rules: dict[str, EnrichmentRule]


# Default enrichment configuration for Hostfully API
HOSTFULLY_ENRICHMENT_CONFIG = EnrichmentConfig(
    rules={
        "leads": EnrichmentRule(
            endpoint_name="leads",
            detail_endpoint_path="leads/{uid}",
            detail_response_key="lead",
            uid_field="uid",
            nested_field_name="assignee",
            nested_uid_field="uid",
            retry_attempts=3,
            retry_delay_seconds=0.5,
        ),
        # Additional endpoints can be added here for future enrichment
        # "properties": EnrichmentRule(...),
        # "messages": EnrichmentRule(...),
    }
)
