"""Hostfully pipeline - Modular architecture for data extraction and enrichment."""

__version__ = "2.0.0"

from .resources import (
    hostfully_rest_api_source,
    threads_incremental,
    fetch_messages_for_lead,
    fetch_messages_from_thread
)
from .utils import (
    lead_uids_from_db,
    check_rate_limit,
    reset_api_counters,
    increment_api_counter,
    generate_pipeline_report,
    get_rows_count_from_db,
    is_first_messages_run,
    property_uids_from_db
)

__all__ = [
    "hostfully_rest_api_source",
    "threads_incremental",
    "fetch_messages_for_lead",
    "fetch_messages_from_thread",
    "lead_uids_from_db",
    "check_rate_limit",
    "reset_api_counters",
    "increment_api_counter",
    "generate_pipeline_report",
    "is_first_messages_run",
    "get_rows_count_from_db",
    "property_uids_from_db"
]
