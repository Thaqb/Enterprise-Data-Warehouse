"""Transformer to fetch BOOKING.COM property reviews"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Iterator, Dict, Any, List, Optional

import dlt
import requests

from config import HostfullyConfig
from hostfully_pipeline.utils import _get_session, increment_api_counter
from enrichment.state_manager import PropertyReviewsStateManager
from enrichment.enrichers import RateLimitError

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30


def property_reviews_booking_factory() -> dlt.transformer:
    """Factory function to create property reviews transformer.
    Returns:
        dlt.transformer for Booking.com reviews
    """
    # Load config inside function
    config = HostfullyConfig.from_dlt()
    
    base_url = config.base_url
    table_name = f"{config.table_prefix}property_reviews_booking"
    
    @dlt.transformer(name='property_reviews_booking', max_table_nesting=0, 
                     table_name=table_name, write_disposition="merge",
                     primary_key=["property_uid"])
    def process_property_reviews(uids: Iterator[str], api_key: str = dlt.secrets.value) -> Iterator[Dict[str, Any]]:
        """Transformer that accepts an iterator or single UID and yields review rows.

        Args:
            uids: iterator of property UIDs or a single UID string
            max_workers: concurrency for per-property fetches
        """
        all_uids = list(uids)
        if not all_uids:
            logger.info("No property UIDs to process for BOOKING.COM reviews.")
            return

        logger.info(f"[PropertyReviewsBooking] Fetching reviews for {len(all_uids)} properties")

        successful_uids: List[str] = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {}
            # Read state on main thread to avoid dlt.state access inside workers
            for uid in all_uids:
                try:
                    last_synced = PropertyReviewsStateManager.get_last_synced(uid)
                    if last_synced == date.today().isoformat():
                        logger.debug(f"[PropertyReviewsBooking] Property {uid} already synced today ({last_synced}), skipping.")
                        continue
                except Exception as e:
                    logger.warning(f"Failed to read last-synced for {uid}: {e}")
                    # proceed to attempt fetch

                futures[executor.submit(_fetch_reviews_for_property, uid, base_url, api_key)] = uid

            for future in as_completed(futures):
                uid = futures[future]
                try:
                    entries = future.result()
                    if entries is None:
                        # skipped due to transient error
                        continue
                    for row in entries:
                        yield row
                    successful_uids.append(uid)
                except RateLimitError as e:
                    # Rate limit hit - stop pipeline immediately
                    logger.error(f"Rate limit hit for property {uid} - STOPPING PIPELINE: {e}")
                    raise
                except Exception as e:
                    # propagate 429/400 HTTP errors to stop the pipeline
                    logger.error(f"Error fetching reviews for {uid}: {e}")
                    raise

        if successful_uids:
            today_iso = date.today().isoformat()
            PropertyReviewsStateManager.mark_synced_batch(successful_uids, today_iso)
    
    return process_property_reviews


def _fetch_reviews_for_property(property_uid: str, base_url: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch all BOOKING.COM reviews for a property using cursor pagination.

    Returns list of review dicts or None on transient skip. Raises HTTP errors including 429.
    """
    session = _get_session(api_key)
    params = {"resourceUid": property_uid}
    collected: List[Dict[str, Any]] = []

    try:

        response = session.get(f"https://platform.hostfully.com/api/internal/bookingdotcom/properties/property-scores", params=params, timeout=REQUEST_TIMEOUT)
        # Track API call
        try:
            increment_api_counter("property_reviews_booking")
        except Exception:
            pass

        if response.status_code == 400:
            logger.error(f"Property: {property_uid} not connected to booking.com")
            return []
        if response.status_code == 429:
            logger.error(f"Rate limit 429 for property_reviews_booking at {property_uid} - STOPPING PIPELINE")
            raise RateLimitError(f"Rate limit hit for property_reviews_booking endpoint (propertyUid={property_uid})")

        response.raise_for_status()
        reviews = response.json() or {}
        reviews['property_uid'] = property_uid
        collected.append(reviews)


        logger.debug(f"Fetched {len(collected)} reviews for {property_uid}")
        return collected

    except requests.exceptions.Timeout as e:
        logger.warning(f"Timeout fetching reviews for {property_uid}: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error fetching reviews for {property_uid}: {e}")
        return None