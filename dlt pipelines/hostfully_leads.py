"""Hostfully Leads Pipeline - Entry point for leads, orders, transactions, and messages extraction.

This pipeline executes in two stages:
1. LEADS + ORDERS + TRANSACTIONS: Fetches and enriches leads with concurrent orders/transactions fetching
2. MESSAGES: Fetches messages (first run via leadUid, subsequent runs via threadUid incremental)

Run this file directly to execute the complete leads pipeline.
"""

import logging
import dlt
import sys
from hostfully_pipeline.resources import (
    hostfully_rest_api_source,
    threads_incremental,
    fetch_messages_for_lead,
    fetch_messages_from_thread,
    create_unified_transformer
)
from hostfully_pipeline.utils import (
    lead_uids_from_db,
    is_first_messages_run
)

logger = logging.getLogger(__name__)
# Configure logging with timestamps for each line
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def run_leads_pipeline():
    """Execute the complete Hostfully leads pipeline.
    
    Stages:
        1. LEADS + ORDERS + TRANSACTIONS (always run)
        2. MESSAGES (first run: via leadUid, subsequent: via threadUid)
    
    Returns:
        Load info from the final stage
    """
    # Read environment mode from config (DEV or PROD)
    try:
        env_mode = dlt.config.get("environment.mode") or "DEV"
    except Exception:
        env_mode = "DEV"
    env_destination = "bigquery" if env_mode == "PROD" else "duckdb"
    env_dataset = f"{env_mode.lower()}_hostfully"
    
    logger.info(f"Pipeline running in {env_mode} mode (destination: {env_destination}, dataset: {env_dataset})")
    
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name='hostfully_pipeline',
        destination=env_destination,
        dataset_name=env_dataset
    )
    
    # =========================================================================
    # STAGE 1: LEADS + ORDERS + TRANSACTIONS
    # =========================================================================
    logger.info("=" * 80)
    logger.info("=== STAGE 1: LEADS + ORDERS + TRANSACTIONS ===")
    logger.info("=" * 80)
    
    source = hostfully_rest_api_source()
    
    # Add transformed leads resource to source using proper dlt pattern
    # This keeps all resources under the same 'hostfully' source/schema
    source.resources.add(source.resources["leads"] | create_unified_transformer())
    
    logger.info("[STAGE 1] Starting pipeline.run()...")
    # Run only the enriched leads transformer (which yields leads, orders, transactions)
    load_info_stage1 = pipeline.run(
        source.resources["enrich_leads"],
        write_disposition="merge"
    )
    
    logger.info(f"[STAGE 1] Completed. Load info:\n{load_info_stage1}")
    
    # =========================================================================
    # STAGE 2: MESSAGES
    # =========================================================================
    
    # Detect if this is first run
    first_run = is_first_messages_run(pipeline.pipeline_name)
    
    # Create a fresh source for messages stage (reuse same source name to keep schema unified)
    messages_source = hostfully_rest_api_source()
    
    if first_run:
        # ---------------------------------------------------------------------
        # FIRST RUN: Fetch messages via leadUid → messages table
        # ---------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("=== STAGE 2: MESSAGES (FIRST RUN - VIA LEAD UIDS) ===")
        logger.info("=" * 80)
        logger.info("Fetching all messages for historical leads to establish baseline...")
        
        messages_from_leads = (
            lead_uids_from_db(pipeline_name=pipeline.pipeline_name) 
            | fetch_messages_for_lead
        )
        
        # Add messages transformer to source to keep under same schema
        messages_source.resources.add(messages_from_leads)
        load_info = pipeline.run(messages_source.resources["messages_from_leads"])
        logger.info(f"[STAGE 2] Messages baseline completed. Load info:\n{load_info}")
        logger.info("Next run will use incremental threads → messages strategy")
    
    else:
        # ---------------------------------------------------------------------
        # SUBSEQUENT RUNS: Fetch threads + messages via threadUid → messages table
        # ---------------------------------------------------------------------
        logger.info("=" * 80)
        logger.info("=== STAGE 2: MESSAGES (SUBSEQUENT - VIA THREAD UIDS) ===")
        logger.info("=" * 80)
        logger.info("Fetching incremental threads and messages...")
        
        # Create chain: threads → messages transformer
        threads = threads_incremental()
        messages_from_threads = threads | fetch_messages_from_thread
        
        # Add to source to keep under same schema
        messages_source.resources.add(messages_from_threads)
        load_info = pipeline.run(messages_source.resources["messages_from_threads"])
        logger.info(f"[STAGE 2] Messages loaded. Load info:\n{load_info}")
    
    # =========================================================================
    # Pipeline Complete
    # =========================================================================
    logger.info("\n" + "=" * 80)
    logger.info("Hostfully Leads Pipeline completed successfully!")
    logger.info("=" * 80)
    
    return load_info


if __name__ == "__main__":
    run_leads_pipeline()
