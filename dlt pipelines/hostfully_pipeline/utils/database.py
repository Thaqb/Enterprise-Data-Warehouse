"""Database utility functions for DuckDB and BigQuery operations."""

import logging
import duckdb
from typing import Iterator, Dict, Any, List, Optional
import dlt

logger = logging.getLogger(__name__)


@dlt.resource(name="lead_uids_from_db")
def lead_uids_from_db(
    pipeline_name: str
) -> Iterator[str]:
    """Extract all lead UIDs from the destination database.
    
    Queries the destination for all distinct lead UIDs to enable fetching messages
    for all historical leads (not just those updated in current run).
    This ensures new messages on old leads are captured.
    
    Behavior depends on environment:
      - Non-PROD (DEV, etc.): reads from local DuckDB file `{pipeline_name}.duckdb`
      - PROD: queries BigQuery using credentials from `dlt.secrets['destination.bigquery.credentials']`
    
    Args:
        pipeline_name: Name of the dlt pipeline (to locate DuckDB file)
        
    Yields:
        Lead UIDs (one at a time for parallel processing)
    """
    # Determine environment mode and dataset
    try:
        env_mode = dlt.config.get("environment.mode") or "DEV"
    except Exception:
        env_mode = "DEV"
    dataset_name = f"{env_mode.lower()}_hostfully"
    
    # DEV: read from local DuckDB
    if env_mode != "PROD":
        db_path = f"{pipeline_name}.duckdb"
        try:
            conn = duckdb.connect(db_path, read_only=True)
            result = conn.execute(
                f"SELECT DISTINCT uid FROM {dataset_name}.raw_leads ORDER BY uid"
            ).fetchall()
            lead_uids = [row[0] for row in result]
            
            logger.info(f"Found {len(lead_uids)} historical leads to fetch messages for (DuckDB)")
            
            # Yield one at a time for parallel processing
            for lead_uid in lead_uids:
                yield lead_uid
                
        except Exception as e:
            logger.warning(f"Could not read lead UIDs from DuckDB: {e}. Skipping messages (first run?).")
            return
        finally:
            if 'conn' in locals():
                conn.close()
    
    # PROD: read from BigQuery
    else:
        try:
            # Lazy import to avoid additional dependency unless needed
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'. Install them to query PROD destination.")
            raise
        
        # Obtain credentials from dlt secrets
        creds_info = None
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None
        
        if not creds_info or not isinstance(creds_info, dict):
            logger.error("BigQuery credentials not found in dlt.secrets['destination.bigquery.credentials']. Cannot query PROD destination.")
            return
        
        project_id = creds_info.get("project_id")
        try:
            credentials = service_account.Credentials.from_service_account_info(creds_info)
            client = bigquery.Client(project=project_id, credentials=credentials)
            table_ref = f"{project_id}.{dataset_name}.raw_leads" if project_id else f"{dataset_name}.raw_leads"
            query = f"SELECT DISTINCT uid FROM `{table_ref}` ORDER BY uid"
            query_job = client.query(query)
            rows = list(query_job.result())
            lead_uids = [r[0] for r in rows]
            
            logger.info(f"Found {len(lead_uids)} historical leads to fetch messages for (BigQuery)")
            
            # Yield one at a time for parallel processing
            for lead_uid in lead_uids:
                yield lead_uid
                
        except Exception as e:
            logger.warning(f"Could not read lead UIDs from BigQuery: {e}. Skipping messages.")
            return


def is_first_messages_run(pipeline_name: str) -> bool:
    """Check if messages table exists in the destination.
    
    Returns True if this is the first run (messages table doesn't exist),
    False if table exists (subsequent run).
    
    Behavior depends on environment:
      - Non-PROD (DEV, etc.): checks DuckDB file `{pipeline_name}.duckdb`
      - PROD: queries BigQuery information schema
    
    Args:
        pipeline_name: Name of the dlt pipeline
        
    Returns:
        bool: True if first run, False if subsequent run
    """
    # Determine environment mode and dataset
    try:
        env_mode = dlt.config.get("environment.mode") or "DEV"
    except Exception:
        env_mode = "DEV"
    dataset_name = f"{env_mode.lower()}_hostfully"
    
    # DEV: check DuckDB
    if env_mode != "PROD":
        try:
            db_path = f"{pipeline_name}.duckdb"
            conn = duckdb.connect(db_path, read_only=True)
            
            # Check if raw_messages table exists
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_messages'"
            ).fetchone()
            
            conn.close()
            
            table_exists = result[0] > 0
            return not table_exists
            
        except Exception as e:
            logger.warning(f"Could not check for messages table in DuckDB: {e}. Assuming first run.")
            return True
    
    # PROD: check BigQuery
    else:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'.")
            return True
        
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None
        
        if not creds_info or not isinstance(creds_info, dict):
            logger.warning("BigQuery credentials not found. Assuming first run.")
            return True
        
        project_id = creds_info.get("project_id")
        try:
            credentials = service_account.Credentials.from_service_account_info(creds_info)
            client = bigquery.Client(project=project_id, credentials=credentials)
            
            # Check if raw_messages table exists
            table_ref = f"{project_id}.{dataset_name}.raw_messages"
            query = f"""
                SELECT COUNT(*) as cnt
                FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = 'raw_messages'
            """
            query_job = client.query(query)
            result = list(query_job.result())
            
            table_exists = result[0][0] > 0 if result else False
            return not table_exists
            
        except Exception as e:
            logger.warning(f"Could not check for messages table in BigQuery: {e}. Assuming first run.")
            return True


def get_rows_count_from_db(pipeline) -> dict:
    """Get row counts for known tables and return a dict of counts.

    Queries the destination database for current row counts. If a table
    doesn't exist (first run), the count for that table will be 0.
    
    Behavior depends on environment:
      - Non-PROD (DEV, etc.): reads from local DuckDB file
      - PROD: queries BigQuery

    Args:
        pipeline: dlt pipeline object

    Returns:
        dict: mapping of table names to counts, e.g.
              {"raw_leads": int, "raw_messages": int, "raw_orders": int,
               "raw_transactions": int, "raw_properties": int}
    """
    counts = {
        "raw_leads": 0,
        "raw_messages": 0,
        "raw_orders": 0,
        "raw_transactions": 0,
        "raw_properties": 0,
    }
    
    # Determine environment mode
    try:
        env_mode = dlt.config.get("environment.mode") or "DEV"
    except Exception:
        env_mode = "DEV"
    
    dataset_name = pipeline.dataset_name
    
    # DEV: read from DuckDB
    if env_mode != "PROD":
        try:
            db_path = f"{pipeline.pipeline_name}.duckdb"
            conn = duckdb.connect(db_path, read_only=True)

            # Get raw_leads count
            try:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM {dataset_name}.raw_leads"
                ).fetchone()
                counts["raw_leads"] = result[0] if result else 0
            except Exception:
                pass

            # Get raw_messages count
            try:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM {dataset_name}.raw_messages"
                ).fetchone()
                counts["raw_messages"] = result[0] if result else 0
            except Exception:
                pass

            # Get raw_orders count
            try:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM {dataset_name}.raw_orders"
                ).fetchone()
                counts["raw_orders"] = result[0] if result else 0
            except Exception:
                pass

            # Get raw_transactions count
            try:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM {dataset_name}.raw_transactions"
                ).fetchone()
                counts["raw_transactions"] = result[0] if result else 0
            except Exception:
                pass

            # Get raw_properties count
            try:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM {dataset_name}.raw_properties"
                ).fetchone()
                counts["raw_properties"] = result[0] if result else 0
            except Exception:
                pass

            conn.close()

        except Exception as e:
            logger.debug(f"Could not get row counts from DuckDB: {e}")
    
    # PROD: read from BigQuery
    else:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'.")
            return counts
        
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None
        
        if not creds_info or not isinstance(creds_info, dict):
            logger.warning("BigQuery credentials not found.")
            return counts
        
        project_id = creds_info.get("project_id")
        try:
            credentials = service_account.Credentials.from_service_account_info(creds_info)
            client = bigquery.Client(project=project_id, credentials=credentials)
            
            tables = ["raw_leads", "raw_messages", "raw_orders", "raw_transactions", "raw_properties"]
            
            for table_name in tables:
                try:
                    table_ref = f"{project_id}.{dataset_name}.{table_name}"
                    query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
                    query_job = client.query(query)
                    result = list(query_job.result())
                    counts[table_name] = result[0][0] if result else 0
                except Exception:
                    # Table doesn't exist or query failed
                    pass
                    
        except Exception as e:
            logger.debug(f"Could not get row counts from BigQuery: {e}")
    
    return counts


@dlt.resource(name="property_uids_from_db")
def property_uids_from_db(pipeline_name: str) -> Iterator[str]:
    """Yield lists of property UIDs known in the pipeline destination.

    Reads distinct `uid` values from the `{env}_hostfully.raw_properties` table
    for the given pipeline. Behavior depends on environment:
      - Non-PROD (DEV, etc.): reads from local DuckDB file `{pipeline_name}.duckdb`.
      - PROD: queries BigQuery using credentials from
        `dlt.secrets['destination.bigquery.credentials']`.

    Args:
        pipeline_name: name of the dlt pipeline (used to locate the DuckDB file)

    Yields:
        list[str]: a single ordered list of distinct property UIDs. If the table
        does not exist or no rows are found (e.g. first run), the generator may
        return without yielding or yield an empty list.
    """
    # Determine environment mode and dataset
    try:
        env_mode = dlt.config.get("environment.mode") or "DEV"
    except Exception:
        env_mode = "DEV"
    dataset_name = f"{env_mode.lower()}_hostfully"

    # DEV: read from local DuckDB
    if env_mode != "PROD":
        db_path = f"{pipeline_name}.duckdb"
        try:
            conn = duckdb.connect(db_path, read_only=True)
            result = conn.execute(f"SELECT DISTINCT uid FROM {dataset_name}.raw_properties ORDER BY uid").fetchall()
            uids = [r[0] for r in result]
            logger.info(f"Found {len(uids)} properties to process (DuckDB)")
            yield uids
        except Exception as e:
            logger.warning(f"Could not read property UIDs from DuckDB: {e}. Is this the first run?")
            return
        finally:
            if 'conn' in locals():
                conn.close()

    # PROD: read from BigQuery
    else:
        try:
            # Lazy import to avoid additional dependency unless needed
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'. Install them to query PROD destination.")
            raise

        # Obtain credentials from dlt secrets
        creds_info = None
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None

        if not creds_info or not isinstance(creds_info, dict):
            logger.error("BigQuery credentials not found in dlt.secrets['destination.bigquery.credentials']. Cannot query PROD destination.")
            return

        project_id = creds_info.get("project_id")
        try:
            credentials = service_account.Credentials.from_service_account_info(creds_info)
            client = bigquery.Client(project=project_id, credentials=credentials)
            table_ref = f"{project_id}.{dataset_name}.raw_properties" if project_id else f"{dataset_name}.raw_properties"
            query = f"SELECT DISTINCT uid FROM `{table_ref}` ORDER BY uid"
            query_job = client.query(query)
            rows = list(query_job.result())
            uids = [r[0] for r in rows]
            logger.info(f"Found {len(uids)} properties to process (BigQuery)")
            yield uids
        except Exception as e:
            logger.warning(f"Could not read property UIDs from BigQuery: {e}")
            return
