"""Factory for creating enrichment transformers.

Provides a factory function that creates @dlt.transformer instances
configured for specific endpoints and enrichment rules.
"""

import logging
from typing import Dict, Any

import dlt

from enrichment.config import EnrichmentRule
from enrichment.state_manager import EnrichmentStateManager
from enrichment.enrichers import (
    is_nested_field_incomplete,
    fetch_detail_with_retry,
    fetch_batch_details_concurrent,
    merge_detail_into_bulk,
)

logger = logging.getLogger(__name__)


def create_enrichment_transformer(
    rule: EnrichmentRule,
    base_url: str,
    headers: Dict[str, str],
):
    """Create a reusable enrichment transformer for an endpoint.
    
    Returns a @dlt.transformer that:
    1. Iterates through items from a parent resource
    2. Detects incomplete nested fields using the rule
    3. Fetches complete data from the detail endpoint (with retries)
    4. Merges detail data back into the item
    5. Tracks enriched UIDs in state for hybrid idempotency
    
    Args:
        rule: EnrichmentRule specifying how to enrich this endpoint
        base_url: API base URL for detail endpoint calls
        headers: HTTP headers for API authentication
        
    Returns:
        A dlt transformer function decorated with @dlt.transformer
    """
    
    @dlt.transformer(name=f"enrich_{rule.endpoint_name}")
    def enrich(items):
        """Transform items by enriching incomplete nested fields using concurrent fetching.
        
        This transformer:
        - Collects all items from parent resource
        - Identifies incomplete nested fields in batch
        - Fetches all detail records concurrently (15 workers)
        - Yields all enriched items
        
        Args:
            items: Iterable of items from parent resource
            
        Yields:
            Enriched items with complete nested fields
        """
        
        # Collect all items and separate into complete vs incomplete
        complete_items = []
        items_needing_enrichment = []
        
        # Get previously enriched UIDs from state (for hybrid idempotency)
        # enriched_uids = EnrichmentStateManager.get_enriched_uids(rule.endpoint_name)

        for item in items:
            uid = item.get(rule.uid_field)

            # Check if this item needs enrichment
            if is_nested_field_incomplete(item, rule):
                items_needing_enrichment.append((item, uid))
            else:
                # No enrichment needed
                complete_items.append(item)
        
        # Log summary
        total_items = len(complete_items) + len(items_needing_enrichment)
        logger.info(
            f"Processing {total_items} {rule.endpoint_name} items: "
            f"{len(complete_items)} complete, {len(items_needing_enrichment)} need enrichment"
        )
        
        
        # Yield complete items first
        for item in complete_items:
            yield item
        
        # If no items need enrichment, we're done
        if not items_needing_enrichment:
            logger.info(f"No {rule.endpoint_name} items require enrichment for this page.")
            return
        
        # Fetch all detail records concurrently
        logger.info(
            f"Starting concurrent enrichment for {len(items_needing_enrichment)} {rule.endpoint_name} records"
        )
        
        import time as _time
        batch_start = _time.time()
        detail_responses = fetch_batch_details_concurrent(
            items_with_uids=items_needing_enrichment,
            detail_endpoint_path=rule.detail_endpoint_path,
            base_url=base_url,
            headers=headers,
            rule=rule,
            max_workers=None,  # read from dlt.config["hostfully"].max_workers if available
        )
        batch_elapsed = _time.time() - batch_start
        
        # Merge detail data into items and yield
        
        for item, uid in items_needing_enrichment:
            detail_response = detail_responses.get(uid)
            enriched_item = merge_detail_into_bulk(item, detail_response, rule)
            
            yield enriched_item
            if detail_response:
                # Mark this UID as enriched in state
                EnrichmentStateManager.add_enriched_uid(rule.endpoint_name, uid)

        logger.info(
            f"Enrichment complete for {rule.endpoint_name}: "
            f"{len(items_needing_enrichment)} records enriched successfully"
        )   
    
    return enrich