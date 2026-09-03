"""Message transformers for Hostfully API."""

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

# Load config inside function
config = HostfullyConfig.from_dlt()

@dlt.transformer(
    name="messages_from_leads",
    write_disposition="merge",
    primary_key=["uid", "threadUid", "leadUid"],
    parallelized=True,
    columns={
        "attachments": {"data_type": "json"}
    },
    max_table_nesting=0,
    table_name=f"{config.table_prefix}messages"
)
def fetch_messages_for_lead(
    lead_uid_or_batch: Any,
    api_key: str = dlt.secrets.value
) -> Iterator[Dict[str, Any]]:
    """Fetch messages for one or more leads (batch) with pagination support.

    Accepts either a single lead UID (str) or a list of UIDs (batch). When a batch
    is provided, the transformer processes them sequentially using a thread-local
    requests.Session to reuse TCP connections and reduce latency.

    Args:
        lead_uid_or_batch: lead UID (str) or list of lead UIDs
        api_key: Hostfully API key for authentication

    Yields:
        Message records with `leadUid` added for foreign key relationship
    """

    
    # Normalize to list of lead UIDs
    if isinstance(lead_uid_or_batch, list):
        lead_uids = lead_uid_or_batch
    else:
        lead_uids = [lead_uid_or_batch]

    base_url = f"{config.base_url}messages"
    # Get a thread-local session (session reuses connections)
    session = _get_session(api_key)
    
    for lead_uid in lead_uids:
        params = {"_limit": 1000, "leadUid": lead_uid}
        cursor = None
        while True:
            if cursor:
                params["_cursor"] = cursor

            try:
                response = session.get(base_url, params=params, timeout=30)
                increment_api_counter("messages")

                # Handle 404 (lead has no messages) - this is normal, skip silently
                if response.status_code == 404:
                    break

                # Handle 429 rate limit
                if response.status_code == 429:
                    logger.error(
                        f"Rate limit (429) hit fetching messages for lead {lead_uid} - STOPPING PIPELINE. "
                        f"Headers: {response.headers}"
                    )
                    raise RateLimitError(f"Rate limit hit for messages endpoint (leadUid={lead_uid})")

                response.raise_for_status()
                data = response.json()

                # Add leadUid to each message (API response doesn't include it)
                messages = data.get("messages", [])
                for message in messages:
                    message["leadUid"] = lead_uid  # Add foreign key!
                    yield message

                # Check for next page
                cursor = data.get("_paging", {}).get("_nextCursor")
                if not cursor:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching messages for lead {lead_uid}: {e}")
                break


@dlt.transformer(
    name="messages_from_threads",
    write_disposition="merge",
    primary_key=["uid", "threadUid", "leadUid"],
    parallelized=True,
    columns={
        "attachments": {"data_type": "json"}
    },
    max_table_nesting=0,
    table_name=f"{config.table_prefix}messages"
)
def fetch_messages_from_thread(
    thread_record: Dict[str, Any],
    api_key: str = dlt.secrets.value
) -> Iterator[Dict[str, Any]]:
    """Fetch messages for a thread and inject leadUid from thread participants.
    
    Only processes threads with LEAD participant (guaranteed by upstream filter).
    Messages response already includes threadUid, we only need to inject leadUid.
    
    Args:
        thread_record: Thread record from threads_incremental resource
        api_key: Hostfully API key
        
    Yields:
        Message records with leadUid injected (threadUid already in response)
    """
    # Load config inside function
    config = HostfullyConfig.from_dlt()
    
    thread_uid = thread_record["uid"]
    
    # Extract leadUid from participants (guaranteed to exist - filtered upstream)
    lead_uid = None
    for participant in thread_record.get("participants", []):
        if participant.get("participantType") == "LEAD":
            lead_uid = participant.get("participantUid")
            break
    
    # Safety check (should never happen due to upstream filter)
    if lead_uid is None:
        logger.warning(
            f"Thread {thread_uid} has no LEAD participant. Skipping messages. "
            f"This should not happen (upstream filter failed)."
        )
        return
    
    # Fetch messages for this thread
    session = _get_session(api_key)
    base_url = f"{config.base_url}messages"
    params = {"_limit": 1000, "threadUid": thread_uid}
    cursor = None
    
    message_count = 0
    
    while True:
        if cursor:
            params["_cursor"] = cursor
        
        try:
            response = session.get(base_url, params=params, timeout=30)
            increment_api_counter("messages")
            
            # Handle 404 (thread has no messages) - normal, skip silently
            if response.status_code == 404:
                logger.debug(f"Thread {thread_uid} has no messages (404)")
                break
            
            # Handle 429 with fail-fast
            if response.status_code == 429:
                logger.error(
                    f"Rate limit hit fetching messages for thread {thread_uid} - STOPPING PIPELINE. "
                    f"Headers: {response.headers}"
                )
                raise RateLimitError(f"Rate limit hit for messages endpoint (threadUid={thread_uid})")
            
            response.raise_for_status()
            data = response.json()
            
            messages = data.get("messages", [])
            
            # Handle empty messages array (200 with no messages) - skip silently
            if not messages:
                logger.debug(f"Thread {thread_uid} has no messages (empty array)")
                break
            
            for message in messages:
                # Inject leadUid (threadUid already in message response)
                message["leadUid"] = lead_uid
                message_count += 1
                yield message
            
            cursor = data.get("_paging", {}).get("_nextCursor")
            if not cursor:
                break
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching messages for thread {thread_uid}: {e}")
            break
    
    logger.debug(f"Fetched {message_count} messages for thread {thread_uid} (leadUid: {lead_uid})")
