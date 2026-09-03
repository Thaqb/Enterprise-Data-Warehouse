"""API helper functions for session management and request tracking."""

import logging
import threading
import requests
import dlt
from datetime import date, datetime
from config import HostfullyConfig

logger = logging.getLogger(__name__)

# Global API call counters (reset at start of each pipeline run)
API_CALL_COUNTERS = {
    "leads": 0,
    "threads": 0,
    "messages": 0,
    "orders": 0,
    "transactions": 0,
    "properties": 0,
    "property_calendar": 0,
    "property_reviews_airbnb": 0,
    "property_reviews_booking": 0,
    "enrichment": 0,
    "rate_limit_checks": 0,
}

# Thread-local storage for per-thread requests.Session
_thread_local = threading.local()


def _get_session(api_key: str) -> requests.Session:
    """Return a thread-local requests.Session with API key header set.
    
    Args:
        api_key: Hostfully API key for authentication
        
    Returns:
        Thread-local requests.Session with API key configured
    """
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        # Optionally configure adapters or timeouts here
        _thread_local.session = session
    # Ensure API key header is current for this run
    session.headers.update({"X-HOSTFULLY-APIKEY": api_key})
    return session


def reset_api_counters():
    """Reset all API call counters to zero."""
    global API_CALL_COUNTERS
    for key in API_CALL_COUNTERS:
        API_CALL_COUNTERS[key] = 0


def increment_api_counter(endpoint: str):
    """Increment the counter for a specific endpoint.
    
    Args:
        endpoint: Name of the endpoint (leads, threads, messages, etc.)
    """
    global API_CALL_COUNTERS
    if endpoint in API_CALL_COUNTERS:
        API_CALL_COUNTERS[endpoint] += 1


def check_rate_limit(api_key: str) -> dict:
    """Check current rate limit status from Hostfully API.
    
    Args:
        api_key: Hostfully API key
        
    Returns:
        dict with rate limit info (remaining, limit)
    """
    # Load config inside function
    config = HostfullyConfig.from_dlt()
    
    try:
        response = requests.get(
            f"{config.base_url}leads",
            headers={"X-HOSTFULLY-APIKEY": api_key},
            params={"_limit": 1},
            timeout=10
        )
        increment_api_counter("rate_limit_checks")
        return {
            "remaining": response.headers.get("x-ratelimit-remaining", "N/A"),
            "limit": response.headers.get("x-ratelimit-limit", "N/A")
        }
    except Exception as e:
        logger.warning(f"Could not check rate limit: {e}")
        return {"remaining": "ERROR", "limit": "N/A"}

def validate_dates(backfill_from, backfill_to):
    if backfill_from and backfill_to:
        # 1. First, validate the format
        try:
            from_param = datetime.fromisoformat(backfill_from)
            to_param = datetime.fromisoformat(backfill_to)
        except ValueError:
            raise ValueError("Dates must be in ISO format (YYYY-MM-DD)")

        # 2. Calculate the gap
        # We use abs() just in case the user puts the "to" date before the "from" date
        delta_days = abs((to_param - from_param).days)

        # 3. The 3-year check
        # 1095 days is exactly 365 * 3. 
        # Using 1096 is safer if you want to allow for a leap year.
        if delta_days > 1096:
            raise ValueError(f"Backfill window is {delta_days} days. It cannot exceed 1096 days (3 years).")
            
    return True