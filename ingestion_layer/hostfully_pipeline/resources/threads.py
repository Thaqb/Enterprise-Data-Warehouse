"""Thread resources for Hostfully API."""

import logging
import dlt
import requests
from typing import Iterator, Dict, Any
from config import HostfullyConfig
from enrichment.enrichers import RateLimitError
from ..utils import (
    _get_session,
    increment_api_counter
)

logger = logging.getLogger(__name__)


@dlt.resource(
    name="threads",
    write_disposition="merge",
    primary_key="uid",
    columns={
        "participants": {"data_type": "json"},
        "participantsReadStatuses": {"data_type": "json"}
    }
)
def threads_incremental(
    api_key: str = dlt.secrets.value,
    last_sync_date: dlt.sources.incremental[str] = dlt.sources.incremental(
        "lastUpdateDate",
        initial_value="2020-01-01T00:00:00Z"
    )
) -> Iterator[Dict[str, Any]]:
    """Fetch threads with client-side incremental filtering.
    
    Since threads endpoint doesn't support updatedSince parameter, we fetch
    pages in DESC order (by lastUpdateDate) and stop when we hit old threads.
    
    Only yields threads with LEAD participant (skips legacy GUEST threads).
    
    Args:
        api_key: Hostfully API key
        last_sync_date: dlt incremental state (last run's max lastUpdateDate)
        
    Yields:
        Thread records with lastUpdateDate > last_sync_date AND has LEAD participant
    """
    # Load config inside function
    config = HostfullyConfig.from_dlt()
    
    base_url = f"{config.base_url}threads"
    session = _get_session(api_key)
    
    params = {
        "_limit": 1000,
        "agencyUid": config.agency_uid
    }
    
    cursor = None
    total_fetched = 0
    total_yielded = 0
    stopped_early = False
    
    logger.info(f"Fetching threads updated since {last_sync_date.start_value}")
    
    while True:
        if cursor:
            params["_cursor"] = cursor
        
        try:
            response = session.get(base_url, params=params, timeout=30)
            increment_api_counter("threads")
            
            # Handle 429 with fail-fast
            if response.status_code == 429:
                logger.error(f"Rate limit hit fetching threads - STOPPING PIPELINE. Headers: {response.headers}")
                raise RateLimitError("Rate limit hit for threads endpoint")
            
            response.raise_for_status()
            data = response.json()
            threads = data.get("threads", [])
            
            if not threads:
                logger.info("No more threads to fetch")
                break
            
            # Client-side incremental filtering
            for thread in threads:
                total_fetched += 1
                thread_update_date = thread.get("lastUpdateDate", "")
                
                # Check if thread is newer than last sync
                if thread_update_date <= last_sync_date.start_value:
                    # Thread is old, stop fetching more pages
                    logger.info(
                        f"Reached thread {thread['uid']} with lastUpdateDate {thread_update_date} "
                        f"<= last sync {last_sync_date.start_value}. Stopping early."
                    )
                    stopped_early = True
                    break
                
                # Only yield threads with LEAD participant (skip legacy GUEST threads)
                has_lead_participant = any(
                    p.get("participantType") == "LEAD"
                    for p in thread.get("participants", [])
                )
                
                if has_lead_participant:
                    yield thread
                    total_yielded += 1
                else:
                    logger.debug(f"Skipping legacy thread {thread['uid']} (no LEAD participant)")
            
            # Stop pagination if we hit old threads
            if stopped_early:
                break
            
            # Get next cursor
            cursor = data.get("_paging", {}).get("_nextCursor")
            if not cursor:
                logger.info("No more pages (no _nextCursor)")
                break
            
            # Log progress every page
            logger.info(
                f"Threads progress: fetched {total_fetched}, yielded {total_yielded} "
                f"(with LEAD participant)"
            )
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching threads: {e}")
            raise
    
    logger.info(
        f"Threads sync complete: {total_yielded} threads yielded out of {total_fetched} fetched. "
        f"Early stop: {stopped_early}"
    )
