"""Unified transformer for concurrent enrichment AND orders fetching.

This transformer processes leads in a single pass, fetching both enrichment
data and orders concurrently within each page, eliminating the sequential
bottleneck of separate transformers.
"""

import logging
import time
from typing import Iterator, Dict, Any, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import dlt
import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError
from enrichment.config import EnrichmentRule, HOSTFULLY_ENRICHMENT_CONFIG
from enrichment.state_manager import EnrichmentStateManager
from enrichment.enrichers import (
    is_nested_field_incomplete,
    fetch_detail_with_retry,
    merge_detail_into_bulk,
    RateLimitError,
)

logger = logging.getLogger(__name__)

# Constants
MAX_WORKERS_ENRICHMENT = 25
MAX_WORKERS_ORDERS = 15
REQUEST_TIMEOUT = 30

# Thread-local session for orders
_thread_local_orders = threading.local()


def _get_orders_session(api_key: str) -> requests.Session:
    """Get thread-local session for orders fetching."""
    session = getattr(_thread_local_orders, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "X-HOSTFULLY-APIKEY": api_key,
            "Content-Type": "application/json"
        })
        _thread_local_orders.session = session
    return session


def create_unified_transformer():
    """Create unified transformer for enrichment + orders.
    
    This transformer:
    1. Collects leads from each page
    2. Concurrently fetches enrichment data AND orders for all leads
    3. Yields enriched leads (to enrich_leads table)
    4. Yields orders (to orders table)
        
    Returns:
        dlt transformer that outputs to multiple tables
    """
    # Load config inside function
    from config import HostfullyConfig
    config = HostfullyConfig.from_dlt()
    base_url = config.base_url
    table_prefix = config.table_prefix
    rule = HOSTFULLY_ENRICHMENT_CONFIG.rules.get("leads")
    
    @dlt.transformer(name="enrich_leads",
                     write_disposition="merge",max_table_nesting=0,primary_key="uid")
    def process_leads_unified(items: Iterator[Dict[str, Any]], api_key: str = dlt.secrets.value) -> Iterator[Dict[str, Any]]:
        """Process leads: enrich and fetch orders concurrently.
        
        For each page of leads:
        1. Collect all items
        2. Identify which need enrichment
        3. Concurrently:
           - Fetch enrichment details (25 workers)
           - Fetch orders for all leads (15 workers)
        4. Yield enriched leads (table: leads)
        5. Yield orders (table: orders)
        
        Args:
            items: Iterator of leads from REST API source
            
        Yields:
            Enriched lead records and order records
        """
        # Create headers here where api_key is guaranteed to be resolved
        headers = {"X-HOSTFULLY-APIKEY": api_key}
        
        page_start = time.time()
        all_items = list(items)
        page_size = len(all_items)
        
        if not all_items:
            return
        
        logger.info(f"[Unified] Processing page with {page_size} leads")
        
        # Separate complete vs incomplete items
        complete_items = []
        items_needing_enrichment = []
        
        for item in all_items:
            uid = item.get(rule.uid_field)
            if is_nested_field_incomplete(item, rule):
                items_needing_enrichment.append((item, uid))
            else:
                complete_items.append(item)
        
        logger.info(
            f"[Unified] {len(complete_items)} complete, "
            f"{len(items_needing_enrichment)} need enrichment"
        )
        
        # Extract all leadUids for orders fetch
        leaduids = [item.get("uid") for item in all_items if item.get("uid")]
        
        # =====================================================================
        # CONCURRENT EXECUTION: Enrichment + Orders in parallel
        # =====================================================================
        enrichment_results = {}
        orders_results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_ENRICHMENT + MAX_WORKERS_ORDERS) as executor:
            futures = {}
            
            # Submit enrichment tasks
            for item, uid in items_needing_enrichment:
                future = executor.submit(
                    _fetch_enrichment_detail,
                    uid, rule, base_url, headers
                )
                futures[future] = ("enrichment", uid)
            
            # Submit orders tasks (one per leadUid)
            for leaduid in leaduids:
                future = executor.submit(
                    _fetch_orders_for_lead,
                    leaduid, base_url, api_key
                )
                futures[future] = ("orders", leaduid)
            
            # Process completed futures
            enrichment_done = 0
            orders_done = 0
            total_enrichment = len(items_needing_enrichment)
            total_orders = len(leaduids)
            
            # Collect transactions separately
            transactions_results = []
            
            for future in as_completed(futures):
                task_type, identifier = futures[future]
                
                try:
                    result = future.result()
                    
                    if task_type == "enrichment":
                        enrichment_results[identifier] = result
                        enrichment_done += 1
                        if enrichment_done % 100 == 0:
                            logger.info(
                                f"[Enrichment] Progress: {enrichment_done}/{total_enrichment}"
                            )
                    else:  # orders
                        if result:
                            # _fetch_orders_for_lead ideally returns (orders_list, transactions_list)
                            if isinstance(result, tuple) and len(result) == 2:
                                orders_list, transactions_list = result
                            elif isinstance(result, list):
                                # Older implementations may return just a list of orders
                                orders_list, transactions_list = result, []
                            else:
                                try:
                                    orders_list, transactions_list = result
                                except Exception:
                                    orders_list, transactions_list = [], []

                            if orders_list:
                                orders_results.extend(orders_list)
                            if transactions_list:
                                transactions_results.extend(transactions_list)
                        orders_done += 1
                        if orders_done % 200 == 0:
                            logger.info(
                                f"[Orders] Progress: {orders_done}/{total_orders}"
                            )
                
                except RateLimitError as e:
                    # Rate limit hit - propagate immediately to stop the pipeline
                    logger.error(f"[Unified] Rate limit hit in {task_type} for {identifier} - STOPPING PIPELINE")
                    raise
                            
                except Exception as e:
                    logger.error(f"[Unified] Error in {task_type} for {identifier}: {e}")
                    if task_type == "enrichment":
                        enrichment_results[identifier] = None
        
        page_duration = time.time() - page_start
        logger.info(
            f"[Unified] Page complete: {enrichment_done} enrichments, "
            f"{len(orders_results)} orders, {len(transactions_results)} transactions in {page_duration:.2f}s"
        )
        
        # =====================================================================
        # YIELD RESULTS
        # =====================================================================
        
        # 1. Yield enriched leads (complete items first, then enriched items)
        # primary_key="uid" is set on the decorator, so no need for hints here
        for item in complete_items:
            yield dlt.mark.with_table_name(item, f"{table_prefix}leads")
        
        for item, uid in items_needing_enrichment:
            detail_response = enrichment_results.get(uid)
            enriched_item = merge_detail_into_bulk(item, detail_response, rule)
            yield dlt.mark.with_table_name(enriched_item, f"{table_prefix}leads")
            # Only mark as enriched if we got a valid detail response
            if detail_response:
                EnrichmentStateManager.add_enriched_uid(rule.endpoint_name, uid)
        
        # 2. Yield orders with table marker and primary key
        for order in orders_results:
            yield dlt.mark.with_hints(
                order,
                dlt.mark.make_hints(table_name=f"{table_prefix}orders", primary_key="uid")
            )
        # 3. Yield transactions with table marker and primary key
        for txn in transactions_results:
            yield dlt.mark.with_hints(
                txn,
                dlt.mark.make_hints(table_name=f"{table_prefix}transactions", primary_key="uid")
            )
    
    return process_leads_unified


def _fetch_enrichment_detail(
    uid: str,
    rule: EnrichmentRule,
    base_url: str,
    headers: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    """Fetch enrichment detail for a single lead."""
    return fetch_detail_with_retry(
        uid=uid,
        detail_endpoint_path=rule.detail_endpoint_path,
        base_url=base_url,
        headers=headers,
        rule=rule
    )


def _fetch_orders_for_lead(
    leaduid: str,
    base_url: str,
    api_key: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch all orders for one leadUid with pagination.
    
    Args:
        leaduid: Lead UID to fetch orders for
        base_url: API base URL  
        api_key: API key
        
    Returns:
        Tuple containing:
            - List of order records with leadUid attached
            - List of transaction records with orderUid attached
    """
    orders = []
    params = {"_limit": 1000, "leadUid": leaduid}
    cursor = None
    session = _get_orders_session(api_key)
    
    transactions = []
    while True:
        if cursor:
            params["_cursor"] = cursor
        
        try:
            response = session.get(
                f"{base_url}orders",
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            # Track API call for orders
            try:
                from hostfully_pipeline.utils.api_helpers import increment_api_counter
                increment_api_counter("orders")
            except Exception:
                pass
            
            if response.status_code == 404:
                # No orders for this lead
                break
            
            if response.status_code == 429:
                logger.error(f"Rate limit 429 for orders {leaduid} - STOPPING PIPELINE")
                raise RateLimitError(f"Rate limit hit for orders endpoint (leadUid={leaduid})")
            
            if response.status_code >= 500:
                logger.warning(f"Server error {response.status_code} for orders {leaduid}")
                break
            
            response.raise_for_status()
            data = response.json()
            
            for order in data.get("orders", []):
                order["leadUid"] = leaduid
                orders.append(order)
                
                # Fetch transactions sequentially for this order
                # RateLimitError will propagate up automatically
                txns_for_order = _fetch_transactions_for_order(order.get("uid"), base_url, api_key)
                if txns_for_order:
                    transactions.extend(txns_for_order)
            
            cursor = data.get("_paging", {}).get("_nextCursor")
            if not cursor:
                break
                
        except RateLimitError:
            # Re-raise rate limit errors to stop pipeline
            raise
        except (Timeout, ConnectionError) as e:
            logger.warning(f"Network error fetching orders for {leaduid}: {e}")
            break
        except Exception as e:
            logger.error(f"Error fetching orders for {leaduid}: {e}")
            break
    
    return orders, transactions


def _fetch_transactions_for_order(order_uid: str, base_url: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetch transactions for a single orderUid (single-page response).

    This endpoint returns transactions for the given `orderUid` and does not
    require cursor-based pagination in the current Hostfully API.

    Args:
        order_uid: Order UID to fetch transactions for
        base_url: API base URL
        api_key: API key

    Returns:
        List of transaction records with `orderUid` field injected
    """
    if not order_uid:
        return []

    txns = []
    params = {"_limit": 1000, "orderUid": order_uid}
    session = _get_orders_session(api_key)

    # Import increment helper to count API calls
    try:
        from hostfully_pipeline.utils.api_helpers import increment_api_counter
    except Exception:
        increment_api_counter = None

    try:
        response = session.get(
            f"{base_url}transactions",
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        # Track API call
        if increment_api_counter:
            try:
                increment_api_counter("transactions")
            except Exception:
                pass

        if response.status_code == 404:
            # No transactions for this order
            return []

        if response.status_code == 429:
            logger.error(f"Rate limit 429 for transactions {order_uid} - STOPPING PIPELINE")
            raise RateLimitError(f"Rate limit hit for transactions endpoint (orderUid={order_uid})")

        if response.status_code >= 500:
            logger.warning(f"Server error {response.status_code} for transactions {order_uid}")
            

        response.raise_for_status()
        data = response.json()

        for txn in data.get("transactions", []):
            txn["orderUid"] = order_uid
            txns.append(txn)

        # Debug: log number of transactions fetched for this order (if any)
        if txns:
            logger.debug(f"[Transactions] Fetched {len(txns)} transactions for order {order_uid}")

    except RateLimitError:
        # Re-raise rate limit errors to stop pipeline
        raise
    except (Timeout, ConnectionError) as e:
        logger.warning(f"Network error fetching transactions for {order_uid}: {e}")
    except Exception as e:
        logger.error(f"Error fetching transactions for {order_uid}: {e}")

    return txns
