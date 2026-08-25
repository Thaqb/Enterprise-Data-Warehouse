"""State management for tracking enriched records.

Manages dlt pipeline state to track which records have been enriched,
enabling hybrid idempotency (avoiding redundant API calls while relying on
merge disposition for correctness).
"""

import logging
from typing import Set, List, Optional

import dlt

logger = logging.getLogger(__name__)


class EnrichmentStateManager:
    """Manages enrichment state in dlt's pipeline state.
    
    Uses dlt.current.resource_state() to store and retrieve which records
    have been enriched, persisting this information across pipeline runs.
    """
    
    STATE_KEY_PREFIX = "enrichment"
    
    @staticmethod
    def get_enriched_uids(endpoint_name: str) -> Set[str]:
        """Retrieve the set of UIDs that have been enriched for an endpoint.
        
        Args:
            endpoint_name: Name of the endpoint (e.g., 'leads')
            
        Returns:
            Set of enriched UIDs for the endpoint, or empty set if none exist
        """
        try:
            state = dlt.current.resource_state()
            key = f"{EnrichmentStateManager.STATE_KEY_PREFIX}_{endpoint_name}_uids"
            enriched_uids = state.get(key, [])
            return set(enriched_uids) if enriched_uids else set()
        except Exception as e:
            logger.warning(f"Failed to retrieve enrichment state for {endpoint_name}: {e}")
            return set()
    
    @staticmethod
    def add_enriched_uid(endpoint_name: str, uid: str) -> None:
        """Add a UID to the set of enriched records for an endpoint.
        
        Args:
            endpoint_name: Name of the endpoint (e.g., 'leads')
            uid: The UID that was enriched
        """
        try:
            state = dlt.current.resource_state()
            key = f"{EnrichmentStateManager.STATE_KEY_PREFIX}_{endpoint_name}_uids"
            enriched_uids = set(state.get(key, []))
            enriched_uids.add(uid)
            state[key] = list(enriched_uids)
            logger.debug(f"Added {uid} to enriched UIDs for {endpoint_name}")
        except Exception as e:
            logger.warning(f"Failed to update enrichment state for {endpoint_name}: {e}")
    
    @staticmethod
    def mark_enriched_batch(endpoint_name: str, uids: list[str]) -> None:
        """Add multiple UIDs to the set of enriched records at once.
        
        Args:
            endpoint_name: Name of the endpoint (e.g., 'leads')
            uids: List of UIDs that were enriched
        """
        try:
            state = dlt.current.resource_state()
            key = f"{EnrichmentStateManager.STATE_KEY_PREFIX}_{endpoint_name}_uids"
            enriched_uids = set(state.get(key, []))
            enriched_uids.update(uids)
            state[key] = list(enriched_uids)
            logger.debug(f"Added {len(uids)} UIDs to enriched set for {endpoint_name}")
        except Exception as e:
            logger.warning(f"Failed to update enrichment batch state for {endpoint_name}: {e}")
    
    @staticmethod
    def clear_enriched_state(endpoint_name: str) -> None:
        """Clear enrichment state for an endpoint (useful for full re-enrichment).
        
        Args:
            endpoint_name: Name of the endpoint (e.g., 'leads')
        """
        try:
            state = dlt.current.resource_state()
            key = f"{EnrichmentStateManager.STATE_KEY_PREFIX}_{endpoint_name}_uids"
            if key in state:
                del state[key]
            logger.info(f"Cleared enrichment state for {endpoint_name}")
        except Exception as e:
            logger.warning(f"Failed to clear enrichment state for {endpoint_name}: {e}")


class PropertyCalendarStateManager:
    """Manages per-property last-synced dates in dlt's pipeline state.

    Stores a mapping of propertyUid -> ISO date string for the last successful
    calendar sync for that property. Provides helpers to get/set single
    property values and to mark batches as synced.
    """

    STATE_KEY = "property_calendar_last_synced"

    @staticmethod
    def get_last_synced(property_uid: str) -> Optional[str]:
        try:
            state = dlt.current.resource_state()
            mapping = state.get(PropertyCalendarStateManager.STATE_KEY, {})
            return mapping.get(property_uid)
        except Exception as e:
            logger.warning(f"Failed to read last-synced state for {property_uid}: {e}")
            return None

    @staticmethod
    def set_last_synced(property_uid: str, iso_date_str: str) -> None:
        try:
            state = dlt.current.resource_state()
            mapping = dict(state.get(PropertyCalendarStateManager.STATE_KEY, {}))
            mapping[property_uid] = iso_date_str
            state[PropertyCalendarStateManager.STATE_KEY] = mapping
            logger.debug(f"Set last-synced for {property_uid} => {iso_date_str}")
        except Exception as e:
            logger.warning(f"Failed to set last-synced state for {property_uid}: {e}")

    @staticmethod
    def mark_synced_batch(uids: List[str], iso_date_str: str) -> None:
        try:
            state = dlt.current.resource_state()
            mapping = dict(state.get(PropertyCalendarStateManager.STATE_KEY, {}))
            for uid in uids:
                mapping[uid] = iso_date_str
            state[PropertyCalendarStateManager.STATE_KEY] = mapping
            logger.debug(f"Marked {len(uids)} properties synced => {iso_date_str}")
        except Exception as e:
            logger.warning(f"Failed to mark batch synced: {e}")


class PropertyReviewsStateManager:
    """Manages per-property last-synced dates for reviews in dlt's state."""

    STATE_KEY = "property_reviews_last_synced"

    @staticmethod
    def get_last_synced(property_uid: str) -> Optional[str]:
        try:
            state = dlt.current.resource_state()
            mapping = state.get(PropertyReviewsStateManager.STATE_KEY, {})
            return mapping.get(property_uid)
        except Exception as e:
            logger.warning(f"Failed to read last-synced state for reviews {property_uid}: {e}")
            return None

    @staticmethod
    def set_last_synced(property_uid: str, iso_date_str: str) -> None:
        try:
            state = dlt.current.resource_state()
            mapping = dict(state.get(PropertyReviewsStateManager.STATE_KEY, {}))
            mapping[property_uid] = iso_date_str
            state[PropertyReviewsStateManager.STATE_KEY] = mapping
            logger.debug(f"Set reviews last-synced for {property_uid} => {iso_date_str}")
        except Exception as e:
            logger.warning(f"Failed to set last-synced state for reviews {property_uid}: {e}")

    @staticmethod
    def mark_synced_batch(uids: List[str], iso_date_str: str) -> None:
        try:
            state = dlt.current.resource_state()
            mapping = dict(state.get(PropertyReviewsStateManager.STATE_KEY, {}))
            for uid in uids:
                mapping[uid] = iso_date_str
            state[PropertyReviewsStateManager.STATE_KEY] = mapping
            logger.debug(f"Marked {len(uids)} properties reviews synced => {iso_date_str}")
        except Exception as e:
            logger.warning(f"Failed to mark batch synced for property reviews: {e}")
