"""Enrichment module for handling incomplete nested data from APIs.

This module provides a reusable framework for detecting and enriching incomplete
nested fields in API responses by fetching additional details from separate endpoints.
"""

from enrichment.config import EnrichmentRule, EnrichmentConfig
from enrichment.transformer_factory import create_enrichment_transformer
# from enrichment.state_manager import EnrichmentStateManager

__all__ = [
    "EnrichmentRule",
    "EnrichmentConfig",
    "create_enrichment_transformer",
    "EnrichmentStateManager",
]
