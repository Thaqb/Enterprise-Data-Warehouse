"""Pipeline reporting utilities."""

import logging
import duckdb
from .api_helpers import API_CALL_COUNTERS
import dlt
from enrichment.config import EnrichmentRule
from enrichment.state_manager import EnrichmentStateManager
logger = logging.getLogger(__name__)

def calculate_total_execution_time(start_time: float, end_time: float) -> float:
    """Calculate total execution time in seconds.
    Args:
        start_time: Pipeline start timestamp
        end_time: Pipeline end timestamp
    Returns:
        Total execution time in seconds
    """
    # Calculate execution time
    total_time = end_time - start_time
    return total_time

def calculate_api_calls_from_rate_limit(
    start_rate_limit: dict,
    end_rate_limit: dict
) -> int:
    """Calculate total API calls made based on rate limit headers.
    
    Args:
        start_rate_limit: Rate limit info at start
        end_rate_limit: Rate limit info at end
        
    Returns:
        Total API calls made during pipeline run
    """
    try:
        api_calls = int(start_rate_limit['remaining']) - int(end_rate_limit['remaining'])
    except (ValueError, TypeError):
        api_calls = -1  # Indicate unknown
    return api_calls

def get_cumulative_enrichment_stats(pipeline) -> int:
    """Retrieve cumulative enrichment statistics from pipeline state.
    
    Args:
        pipeline: dlt Pipeline object
        
    Returns:
        int: enriched_total
    """
    try:
        # Access the pipeline state
        pipeline_state = pipeline.state
        
        # Navigate to the enrichment stats in state
        sources = pipeline_state.get("sources", {})
        hostfully = sources.get("hostfully", {})
        resources = hostfully.get("resources", {})
        enrich_leads=resources.get("enrich_leads", {})
        enrichment_leads_uids = enrich_leads.get("enrichment_leads_uids", [])
        
        return len(enrichment_leads_uids) 
        
    except Exception as e:
        logger.warning(f"Could not retrieve enrichment_leads_uids from state: {e}")
        return 0

def generate_pipeline_report(
    start_time: float,
    end_time: float,
    start_rate_limit: dict,
    end_rate_limit: dict,
    pipeline,
    leads_count_before=0,
    messages_count_before=0,
    orders_count_before=0,
    transactions_count_before=0,
    properties_count_before=0,
    leads_count_after=0,
    messages_count_after=0,
    orders_count_after=0,
    transactions_count_after=0,
    properties_count_after=0,
    stage1_trace=None,
    stage2_trace=None,
) -> str:
    """Generate a simple structured table report of pipeline execution.
    
    Args:
        start_time: Pipeline start timestamp
        end_time: Pipeline end timestamp
        start_rate_limit: Rate limit info at start
        end_rate_limit: Rate limit info at end
        pipeline: dlt Pipeline object
        leads_count_before: Number of leads in DB before run
        messages_count_before: Number of messages in DB before run
        orders_count_before: Number of orders in DB before run
        leads_count_after: Number of leads in DB after run
        messages_count_after: Number of messages in DB after run
        orders_count_after: Number of orders in DB after run
        stage1_trace: dlt Trace object for stage 1
        stage2_trace: dlt Trace object for stage 2
    Returns:
        Formatted report string
    """
    
    # Calculate total execution time
    total_time = calculate_total_execution_time(start_time, end_time)

    # Calculate API calls from rate limit headers (most accurate - from API server)
    api_calls_from_rate_limit = calculate_api_calls_from_rate_limit(start_rate_limit, end_rate_limit)
    
    # Get tracked API calls (from our counters - may miss some dlt internal calls)
    total_tracked = sum(API_CALL_COUNTERS.values())
    
    # Calculate ROWS PROCESSED (inserts + updates) from load_info
    leads_processed = stage1_trace.last_normalize_info.row_counts.get('raw_leads', 0) if stage1_trace else 0
    orders_processed = stage1_trace.last_normalize_info.row_counts.get('raw_orders', 0) if stage1_trace else 0
    transactions_processed = stage1_trace.last_normalize_info.row_counts.get('raw_transactions', 0) if stage1_trace else 0
    properties_processed = stage1_trace.last_normalize_info.row_counts.get('raw_properties', 0) if stage1_trace else 0
    messages_processed = stage2_trace.last_normalize_info.row_counts.get('raw_messages', 0) if stage2_trace else 0
    
    # Calculate rows loaded THIS run (difference between after and before)
    leads_net_new = max(0, leads_count_after - leads_count_before)
    messages_net_new = max(0, messages_count_after - messages_count_before)
    orders_net_new = max(0, orders_count_after - orders_count_before)
    transactions_net_new = max(0, transactions_count_after - transactions_count_before)
    properties_net_new = max(0, properties_count_after - properties_count_before)
    
    # Format the report
    report = "\n" + "=" * 80 + "\n"
    report += "PIPELINE EXECUTION REPORT\n"
    report += "=" * 80 + "\n\n"
    
    # Execution Time
    report += f"{'Metric':<40} {'Value':>35}\n"
    report += "-" * 80 + "\n"
    report += f"{'Total Execution Time':<40} {total_time:>32.2f}s\n"
    report += "\n"
    
    # API Usage with breakdown
    report += "API CALLS:\n"
    report += f"{'  Total API Calls Made':<40} {str(api_calls_from_rate_limit):>35}\n"
    report += f"{'    (from API rate limit headers)':<40}\n"
    report += "\n"
    report += f"{'  Self-Tracked Calls':<40} {total_tracked:>35}\n"
    report += f"{'    (may not include dlt internal)':<40}\n"
    report += "\n"
    report += "  Breakdown by Endpoint:\n"
    report += f"{'    - Threads':<40} {API_CALL_COUNTERS['threads']:>35}\n"
    report += f"{'    - Messages':<40} {API_CALL_COUNTERS['messages']:>35}\n"
    report += f"{'    - Orders':<40} {API_CALL_COUNTERS['orders']:>35}\n"
    report += f"{'    - Transactions':<40} {API_CALL_COUNTERS['transactions']:>35}\n"
    report += f"{'    - Properties':<40} {API_CALL_COUNTERS['properties']:>35}\n"
    report += f"{'    - Property Calendar':<40} {API_CALL_COUNTERS.get('property_calendar', 0):>35}\n"
    report += f"{'    - Property Reviews (Airbnb)':<40} {API_CALL_COUNTERS.get('property_reviews_airbnb', 0):>35}\n"
    report += f"{'    - Enrichment':<40} {API_CALL_COUNTERS['enrichment']:>35}\n"
    report += f"{'    - Rate Limit Checks':<40} {API_CALL_COUNTERS['rate_limit_checks']:>35}\n"
    report += "\n"
    report += f"{'  Note: Leads and Orders endpoint calls made by dlt (not tracked separately)':<80}\n"
    report += "\n"
    
    # Data Loaded This Run
    report += "STATS OF THIS RUN:\n"
    
    # Leads breakdown: updated vs newly inserted
    leads_updated = leads_processed - leads_net_new if leads_processed > leads_net_new else 0
    leads_inserted = leads_net_new
    
    report += f"{'  Leads':<40} {leads_processed:>35,}\n"
    report += f"{'    └─ Updated':<40} {leads_updated:>35,}\n"
    report += f"{'    └─ Newly inserted':<40} {leads_inserted:>35,}\n"
    report += "\n"
    report += f"{'  Enriched leads/run':<40} {API_CALL_COUNTERS['enrichment']:>35,}\n"
    report += "\n"
    
    # Messages (immutable - only inserts)
    report += f"{'  Messages (inserted)':<40} {messages_net_new:>35,}\n"
    if messages_processed > messages_net_new:
        messages_duplicates = messages_processed - messages_net_new
        report += f"{'    └─ duplicates filtered':<40} {messages_duplicates:>35,}\n"
    
    report += "\n"
    
    # Orders (inserted)
    report += f"{'  Orders (inserted)':<40} {orders_net_new:>35,}\n"
    report += f"{'  Transactions (inserted)':<40} {transactions_net_new:>35,}\n"
    report += f"{'  Properties (inserted)':<40} {properties_net_new:>35,}\n"
    
    report += "\n"
    
    # Total Data in Database
    report += "TOTAL DATA IN DATABASE:\n"
    
    # Get CUMULATIVE enrichment stats for total database view
    enriched_total = get_cumulative_enrichment_stats(pipeline)
    not_enriched_total = leads_count_after - enriched_total
    
    report += f"{'  Leads':<40} {leads_count_after:>35,}\n"
    report += f"{'    └─ enriched':<40} {enriched_total:>35,}\n"
    report += f"{'    └─ not enriched':<40} {not_enriched_total:>35,}\n"
    report += "\n"
    report += f"{'  Orders':<40} {orders_count_after:>35,}\n"
    report += f"{'  Transactions':<40} {transactions_count_after:>35,}\n"
    report += "\n"
    report += f"{'  Messages':<40} {messages_count_after:>35,}\n"
    report += f"{'  Properties':<40} {properties_count_after:>35,}\n"
    report += "\n"
    
    # Rate Limit Status
    report += "RATE LIMIT STATUS:\n"
    report += f"{'  Start':<40} {f'{start_rate_limit["remaining"]} / {start_rate_limit["limit"]}':>35}\n"
    report += f"{'  End':<40} {f'{end_rate_limit["remaining"]} / {end_rate_limit["limit"]}':>35}\n"
    
    report += "=" * 80 + "\n"
    
    return report
