"""Core enrichment logic for detecting and fetching incomplete nested data.

Provides functions to detect incomplete nested fields and enrich them with
data from detail endpoints.
"""

import logging
import time
import threading
from typing import Any, Dict, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import dlt
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError
from enrichment.config import EnrichmentRule
from config import HostfullyConfig


class RateLimitError(Exception):
    """Exception raised when API rate limit (429) is hit.
    
    This exception is designed to propagate through concurrent executors
    and stop the pipeline immediately rather than being caught by generic
    exception handlers.
    """
    pass

# Import API counter from hostfully_pipeline if available
try:
    from hostfully_pipeline.utils import increment_api_counter
    _counter_available = True
except ImportError:
    _counter_available = False

# Thread-local session for concurrent enrichment requests
_thread_local_enrich = threading.local()

def _get_enrich_session(headers: Dict[str, str]) -> requests.Session:
    """Return a thread-local requests.Session for enrichment fetches."""
    session = getattr(_thread_local_enrich, "session", None)
    if session is None:
        session = requests.Session()
        _thread_local_enrich.session = session
    # update headers (API key) in case they changed
    if headers:
        session.headers.update(headers)
    return session


logger = logging.getLogger(__name__)


def is_nested_field_incomplete(item: Dict[str, Any], rule: EnrichmentRule) -> bool:
    """Check if a nested field in an item requires enrichment.
    
    Detects two cases:
    1. nested_field is None (no assignee exists) - doesn't need enrichment
    2. nested_field exists but the uid sub-field is None - needs enrichment
    
    Args:
        item: The record to check
        rule: The enrichment rule for this endpoint
        
    Returns:
        True if the nested field exists but its uid is None, False otherwise
    """
    nested_field = item.get(rule.nested_field_name)
    
    # Case 1: nested_field is None (acceptable - no assignee)
    if nested_field is None:
        return False
    
    # Case 2: nested_field exists but uid is None (needs enrichment)
    if isinstance(nested_field, dict):
        uid_value = nested_field.get(rule.nested_uid_field)
        if uid_value is None:
            return True
    
    return False


def fetch_detail_with_retry(
    uid: str,
    detail_endpoint_path: str,
    base_url: str,
    headers: Dict[str, str],
    rule: EnrichmentRule,
) -> Optional[Dict[str, Any]]:
    """Fetch detail data from the detail endpoint with retry logic.
    
    Implements exponential retry with 2-second delays. Falls back to None
    on all retries exhausted, allowing the original bulk data to be used.
    
    Args:
        uid: The unique identifier of the record to fetch
        detail_endpoint_path: Template path for detail endpoint (e.g., 'leads/{uid}')
        base_url: API base URL
        headers: HTTP headers for authentication
        rule: The enrichment rule containing retry configuration
        
    Returns:
        Parsed JSON response from detail endpoint, or None if all retries fail
    """
    endpoint = detail_endpoint_path.format(uid=uid)
    url = f"{base_url}{endpoint}"

    for attempt in range(1, rule.retry_attempts + 1):
        try:
            logger.debug(f"Fetching detail for {rule.endpoint_name} UID {uid} (attempt {attempt}/{rule.retry_attempts})")

            session = _get_enrich_session(headers)
            response = session.get(url, timeout=10)
            
            # Track enrichment API calls
            if _counter_available:
                increment_api_counter("enrichment")

            response.raise_for_status()

            logger.debug(f"Successfully fetched detail for {rule.endpoint_name} UID {uid}")
            return response.json()
            
        except (Timeout, ConnectionError) as e:
            logger.warning(
                f"Timeout/Connection error fetching {rule.endpoint_name} detail for UID {uid} "
                f"(attempt {attempt}/{rule.retry_attempts}): {type(e).__name__}"
            )
            if attempt < rule.retry_attempts:
                time.sleep(rule.retry_delay_seconds)
                
        except HTTPError as e:
            if response.status_code == 404:
                logger.warning(
                    f"Detail endpoint returned 404 for {rule.endpoint_name} UID {uid} "
                    f"(attempt {attempt}/{rule.retry_attempts}). Record may have been deleted."
                )
                # Don't retry on 404 - record doesn't exist
                return None
            elif response.status_code == 429:
                logger.error(
                    f"Rate limit (429) hit for {rule.endpoint_name} UID {uid} - STOPPING PIPELINE. "
                    f"Headers: {response.headers}"
                )
                raise RateLimitError(f"Rate limit hit for {rule.endpoint_name} endpoint (uid={uid})")
            else:
                logger.warning(
                    f"HTTP error {response.status_code} fetching {rule.endpoint_name} detail for UID {uid} "
                    f"(attempt {attempt}/{rule.retry_attempts})"
                )
                if attempt < rule.retry_attempts:
                    time.sleep(rule.retry_delay_seconds)
        
        except RateLimitError:
            # Re-raise rate limit errors to propagate up
            raise
                    
        except Exception as e:
            logger.error(
                f"Unexpected error fetching {rule.endpoint_name} detail for UID {uid} "
                f"(attempt {attempt}/{rule.retry_attempts}): {e}"
            )
            if attempt < rule.retry_attempts:
                time.sleep(rule.retry_delay_seconds)
    
    logger.error(
        f"Failed to fetch detail for {rule.endpoint_name} UID {uid} after {rule.retry_attempts} attempts. "
        f"Using original bulk data with incomplete fields."
    )
    return None


def merge_detail_into_bulk(
    bulk_item: Dict[str, Any],
    detail_data: Dict[str, Any],
    rule: EnrichmentRule,
) -> Dict[str, Any]:
    """Merge detail response data into the bulk item.
    
    Replaces incomplete fields in bulk_item with complete data from detail response.
    
    Args:
        bulk_item: The original record from bulk endpoint
        detail_data: The parsed JSON response from detail endpoint
        rule: The enrichment rule for this endpoint
        
    Returns:
        The merged item with enriched nested fields
    """
    if not detail_data:
        return bulk_item
    
    try:
        # Extract the actual object from the response (might be wrapped under a key)
        detail_object = detail_data.get(rule.detail_response_key, detail_data)
        
        # Copy the nested field from detail into bulk item
        if rule.nested_field_name in detail_object:
            bulk_item[rule.nested_field_name] = detail_object[rule.nested_field_name]
            logger.debug(
                f"Enriched {rule.endpoint_name} UID {bulk_item.get(rule.uid_field)} "
                f"with data from detail endpoint"
            )
        
        return bulk_item
        
    except Exception as e:
        logger.error(f"Error merging detail data for {rule.endpoint_name}: {e}")
        return bulk_item


def fetch_batch_details_concurrent(
    items_with_uids: List[Tuple[Dict[str, Any], str]],
    detail_endpoint_path: str,
    base_url: str,
    headers: Dict[str, str],
    rule: EnrichmentRule,    config: HostfullyConfig,    max_workers: Optional[int] = None,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """Fetch multiple detail records concurrently using ThreadPoolExecutor.

    Fetches detail data for multiple UIDs in parallel, respecting API rate limits.
    If `max_workers` is not provided, the value is read from dlt config:
    `dlt.config.get("hostfully", {}).get("max_workers", 15)`.

    Args:
        items_with_uids: List of (item, uid) tuples to enrich
        detail_endpoint_path: Template path for detail endpoint
        base_url: API base URL
        headers: HTTP headers for authentication
        rule: Enrichment rule with retry configuration
        max_workers: Number of concurrent requests (optional, overrides config)

    Returns:
        Dict mapping uid -> detail_response (or None if failed)
    """
    # Load config inside function if max_workers not provided
    if max_workers is None:
        config = HostfullyConfig.from_dlt()
        max_workers = config.max_workers
    
    effective_max_workers = max_workers
    results = {}
    
    def fetch_single_detail(uid: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Wrapper to fetch detail for a single UID."""
        detail = fetch_detail_with_retry(
            uid=uid,
            detail_endpoint_path=detail_endpoint_path,
            base_url=base_url,
            headers=headers,
            rule=rule,
        )
        return uid, detail
    
    # Extract unique UIDs
    uids_to_fetch = list(set(uid for _, uid in items_with_uids))
    total_uids = len(uids_to_fetch)
    
    logger.info(
        f"Starting concurrent enrichment for {total_uids} {rule.endpoint_name} records "
        f"using {effective_max_workers} workers"
    )
    
    # Fetch concurrently with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=effective_max_workers) as executor:
        # Submit all fetch tasks
        future_to_uid = {executor.submit(fetch_single_detail, uid): uid for uid in uids_to_fetch}
        
        completed_count = 0
        rate_limit_hit = False
        
        # Process completed futures as they finish
        for future in as_completed(future_to_uid):
            uid = future_to_uid[future]
            try:
                fetched_uid, detail_data = future.result()
                results[fetched_uid] = detail_data
                completed_count += 1
                
                # Log progress every 50 items
                if completed_count % 50 == 0:
                    logger.info(
                        f"Concurrent enrichment progress: {completed_count}/{total_uids} "
                        f"({completed_count*100//total_uids}%)"
                    )
                    
            except Exception as e:
                logger.error(f"Unexpected error in concurrent fetch for UID {uid}: {e}")
                results[uid] = None
    
    logger.info(
        f"Concurrent enrichment complete: {completed_count}/{total_uids} records processed, "
        f"{sum(1 for v in results.values() if v is not None)} successful"
    )
    
    return results


def get_base_url_and_headers(source_func) -> tuple[str, Dict[str, str]]:
    """Extract base_url and headers from dlt source function.
    
    This retrieves the client configuration needed for detail endpoint calls.
    
    Args:
        source_func: The dlt source function (e.g., hostfully_rest_api_source)
        config: Hostfully configuration
        
    Returns:
        Tuple of (base_url, headers) used by the REST API source
        
    Raises:
        ValueError: If unable to determine base_url or headers
    """
    try:
        # Load config inside function
        config = HostfullyConfig.from_dlt()
        api_key = dlt.secrets.get("api_key")
        base_url = config.base_url
        headers = {"X-HOSTFULLY-APIKEY": api_key} if api_key else {}
        return base_url, headers
    except Exception as e:
        logger.error(f"Failed to extract base_url and headers from source: {e}")
        raise ValueError("Unable to determine API base_url and headers for detail endpoint") from e

