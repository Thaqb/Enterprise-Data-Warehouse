"""Property calendar transformer for Hostfully API."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Iterator, Dict, Any, List, Optional
import dlt
import requests
from config import HostfullyConfig
from hostfully_pipeline.utils import _get_session, increment_api_counter, validate_dates
from enrichment.state_manager import PropertyCalendarStateManager
from enrichment.enrichers import RateLimitError
logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30
def property_calendar_transformer_factory(
    backfill_from: Optional[str] = None,
    backfill_to: Optional[str] = None,
    max_workers: Optional[int] = None
) -> dlt.transformer:
    """Factory that returns a dlt.transformer which fetches property calendars concurrently.

    Args:
        backfill_from/backfill_to: optional backfill window to use when a property has no last-synced state.
            If None, uses values from config.property_calendar_backfill_from/to
        max_workers: optional number of concurrent threads for fetching. If None, uses config.max_workers

    Returns:
        dlt.transformer
    """
    # Load config inside function
    config = HostfullyConfig.from_dlt()
    
    # Resolve defaults from config
    effective_backfill_from = backfill_from or config.property_calendar_backfill_from
    effective_backfill_to = backfill_to or config.property_calendar_backfill_to
    effective_max_workers = max_workers or config.max_workers
    base_url = config.base_url
    table_name = f"{config.table_prefix}property_calendar"
    
    # Validate backfill dates if provided
    if effective_backfill_from and effective_backfill_to:
        try:
            validate_dates(effective_backfill_from, effective_backfill_to)
        except ValueError as ve:
            logger.error(f"Invalid backfill dates configuration: {ve}")
            raise
    @dlt.transformer(name="property_calendar", 
                     write_disposition="merge",
                     primary_key=["property_uid", "date"],
                     table_name=table_name)
    def process_property_calendar(uids: Iterator[str], api_key: str = dlt.secrets.value) -> Iterator[Dict[str, Any]]:
        
        all_uids = list(uids)
        
        if not all_uids:
            logger.info("No property UIDs to process for calendar.")
            return

        logger.info(f"[PropertyCalendar] Fetching calendars for batch of {len(all_uids)} properties")

        successful_uids = []
        with ThreadPoolExecutor(max_workers=effective_max_workers) as executor:
            futures = {}
            # Determine per-property date windows in this (main) thread so we access dlt state safely
            try:
                for uid in all_uids:
                    last_synced = PropertyCalendarStateManager.get_last_synced(uid)
                    if last_synced:
                        logger.debug(f"[PropertyCalendar] Property {uid} last synced on {last_synced}")
                        f_from, f_to = last_synced, date.today().isoformat()
                        if f_from == f_to:
                            logger.info(f"[PropertyCalendar] Property {uid} already synced up to date {f_to}, skipping.")
                            continue
                    elif effective_backfill_from and effective_backfill_to:
                        f_from, f_to = effective_backfill_from, effective_backfill_to
                    else:
                        # No last synced and no backfill config; skip this property
                        logger.error(f"[PropertyCalendar] No last-synced date or backfill config for {uid}")
                        raise Exception(f"No last-synced date or backfill config for {uid}")

                    futures[executor.submit(_fetch_property_calendar, uid, base_url, api_key, f_from, f_to)] = uid
            except Exception as e:
                logger.error(f"Failed to compute window for {uid}: {e}")    
                raise SystemExit(f"Script terminated: {e}.") from e
            
            for future in as_completed(futures):
                uid = futures[future]
                try:
                    entries = future.result()
                    if entries is None:
                        # skipped or handled (e.g., network error)
                        continue
                    # entries: list of rows for this property
                    for row in entries:
                        yield row
                    successful_uids.append(uid)
                except RateLimitError as e:
                    # Rate limit hit - stop pipeline immediately
                    logger.error(f"Rate limit hit for property {uid} - STOPPING PIPELINE: {e}")
                    raise
                except Exception as e:
                    # On other errors we want the pipeline to stop (exception propagates)
                    logger.error(f"Error fetching calendar for {uid}: {e}")
                    raise

        # If this batch had successful property fetches, mark them synced with today's date
        if successful_uids:
            PropertyCalendarStateManager.mark_synced_batch(successful_uids, f_to)

    # Apply decorator at return time to preserve factory function signature
    return process_property_calendar


def _fetch_property_calendar(property_uid: str, 
                             base_url: str, 
                             api_key: str, 
                             from_iso: str, 
                             to_iso: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch calendar entries for a single property in the provided from->to window.

    Args:
        property_uid: property UID
        base_url: Hostfully API base URL
        api_key: API key
        from_iso: ISO date string for `from` param
        to_iso: ISO date string for `to` param

    Returns a list of rows or None on skip. Raises on 429 or 400 range errors to stop the pipeline.
    """

    session = _get_session(api_key)

    params = {"from": from_iso, "to": to_iso}

    try:
        response = session.get(f"{base_url}property-calendar/{property_uid}", params=params, timeout=REQUEST_TIMEOUT)
        # Track API call
        try:
            increment_api_counter("property_calendar")
        except Exception:
            pass

        if response.status_code == 429:
            # Stop pipeline immediately per project policy
            logger.error(f"Rate limit 429 for property_calendar at {property_uid} - STOPPING PIPELINE")
            raise RateLimitError(f"Rate limit hit for property_calendar endpoint (propertyUid={property_uid})")

        if response.status_code == 400:
           logger.error(f"Calendar range error for {property_uid}")

        response.raise_for_status()
        data = response.json()
        entries = data.get("calendar", {}).get("entries", [])

        for entry in entries:
            # ensure propertyUid is present on each entry for downstream schema
            if "propertyUid" not in entry:
                entry["propertyUid"] = property_uid

        logger.debug(f"Fetched {len(entries)} calendar entries for {property_uid} ({from_iso} -> {to_iso})")
        return entries

    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout fetching calendar for {property_uid}: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error fetching calendar for {property_uid}: {e}")
        return None
