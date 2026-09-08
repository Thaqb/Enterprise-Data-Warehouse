"""Database utility functions for DuckDB and BigQuery operations."""

import logging
import duckdb
from typing import Iterator, Dict, Any, List, Optional
import dlt

from config.conf_pipeline import PipelineConfig

logger = logging.getLogger(__name__)


@dlt.resource(name="lead_uids_from_db")
def lead_uids_from_db(
    pipeline_name: str,
    pipeline_config: PipelineConfig,
) -> Iterator[str]:
    """Extract all lead UIDs from the destination database.
    
    Queries the destination for all distinct lead UIDs to enable fetching messages
    for all historical leads (not just those updated in current run).
    This ensures new messages on old leads are captured.
    
    Behavior depends on environment:
      - dev: reads from local DuckDB file `{pipeline_name}.duckdb`
      - prod: queries BigQuery using credentials from `dlt.secrets['destination.bigquery.credentials']`
    
    Args:
        pipeline_name: Name of the dlt pipeline (to locate DuckDB file)
        pipeline_config: Resolved runtime configuration (environment/destination/dataset)
        
    Yields:
        Lead UIDs (one at a time for parallel processing)
    """
    dataset_name = pipeline_config.dataset

    # dev: read from local DuckDB
    if pipeline_config.destination != "bigquery":
        db_path = f"{pipeline_name}.duckdb"
        try:
            conn = duckdb.connect(db_path, read_only=True)
            result = conn.execute(
                f"SELECT DISTINCT uid FROM {dataset_name}.raw_leads ORDER BY uid"
            ).fetchall()
            lead_uids = [row[0] for row in result]
            
            logger.info(f"Found {len(lead_uids)} historical leads to fetch messages for (DuckDB)")
            
            for lead_uid in lead_uids:
                yield lead_uid
                
        except Exception as e:
            logger.warning(f"Could not read lead UIDs from DuckDB: {e}. Skipping messages (first run?).")
            return
        finally:
            if 'conn' in locals():
                conn.close()
    
    # prod: read from BigQuery
    else:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'. Install them to query prod destination.")
            raise
        
        creds_info = None
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None
        
        if not creds_info or not isinstance(creds_info, dict):
            logger.error("BigQuery credentials not found in dlt.secrets['destination.bigquery.credentials']. Cannot query prod destination.")
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
            
            for lead_uid in lead_uids:
                yield lead_uid
                
        except Exception as e:
            logger.warning(f"Could not read lead UIDs from BigQuery: {e}. Skipping messages.")
            return


def is_first_messages_run(pipeline_name: str, pipeline_config: PipelineConfig) -> bool:
    """Check if messages table exists in the destination.
    
    Returns True if this is the first run (messages table doesn't exist),
    False if table exists (subsequent run).
    
    Behavior depends on environment:
      - dev: checks DuckDB file `{pipeline_name}.duckdb`
      - prod: queries BigQuery information schema
    
    Args:
        pipeline_name: Name of the dlt pipeline
        pipeline_config: Resolved runtime configuration (environment/destination/dataset)
        
    Returns:
        bool: True if first run, False if subsequent run
    """
    dataset_name = pipeline_config.dataset

    # dev: check DuckDB
    if pipeline_config.destination != "bigquery":
        try:
            db_path = f"{pipeline_name}.duckdb"
            conn = duckdb.connect(db_path, read_only=True)
            
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_messages'"
            ).fetchone()
            
            conn.close()
            
            table_exists = result[0] > 0
            return not table_exists
            
        except Exception as e:
            logger.warning(f"Could not check for messages table in DuckDB: {e}. Assuming first run.")
            return True
    
    # prod: check BigQuery
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


def get_rows_count_from_db(pipeline, pipeline_config: PipelineConfig) -> dict:
    """Get row counts for known tables and return a dict of counts.

    Queries the destination database for current row counts. If a table
    doesn't exist (first run), the count for that table will be 0.
    
    Behavior depends on environment:
      - dev: reads from local DuckDB file
      - prod: queries BigQuery

    Args:
        pipeline: dlt pipeline object
        pipeline_config: Resolved runtime configuration (environment/destination/dataset)

    Returns:
        dict: mapping of table names to counts
    """
    counts = {
        "raw_leads": 0,
        "raw_messages": 0,
        "raw_orders": 0,
        "raw_transactions": 0,
        "raw_properties": 0,
    }

    dataset_name = pipeline.dataset_name

    # dev: read from DuckDB
    if pipeline_config.destination != "bigquery":
        try:
            db_path = f"{pipeline.pipeline_name}.duckdb"
            conn = duckdb.connect(db_path, read_only=True)

            for table_name in counts.keys():
                try:
                    result = conn.execute(
                        f"SELECT COUNT(*) FROM {dataset_name}.{table_name}"
                    ).fetchone()
                    counts[table_name] = result[0] if result else 0
                except Exception:
                    pass

            conn.close()

        except Exception as e:
            logger.debug(f"Could not get row counts from DuckDB: {e}")
    
    # prod: read from BigQuery
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

            for table_name in counts.keys():
                try:
                    table_ref = f"{project_id}.{dataset_name}.{table_name}"
                    query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
                    query_job = client.query(query)
                    result = list(query_job.result())
                    counts[table_name] = result[0][0] if result else 0
                except Exception:
                    pass
                    
        except Exception as e:
            logger.debug(f"Could not get row counts from BigQuery: {e}")
    
    return counts


@dlt.resource(name="property_uids_from_db")
def property_uids_from_db(
    pipeline_name: str,
    pipeline_config: PipelineConfig,
) -> Iterator[str]:
    """Yield lists of property UIDs known in the pipeline destination.

    Reads distinct `uid` values from the `{dataset}.raw_properties` table.
    Behavior depends on environment:
      - dev: reads from local DuckDB file `{pipeline_name}.duckdb`.
      - prod: queries BigQuery using credentials from
        `dlt.secrets['destination.bigquery.credentials']`.

    Args:
        pipeline_name: name of the dlt pipeline (used to locate the DuckDB file)
        pipeline_config: Resolved runtime configuration (environment/destination/dataset)

    Yields:
        list[str]: a single ordered list of distinct property UIDs.
    """
    dataset_name = pipeline_config.dataset

    # dev: read from local DuckDB
    if pipeline_config.destination != "bigquery":
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

    # prod: read from BigQuery
    else:
        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("BigQuery support requires 'google-cloud-bigquery' and 'google-auth'. Install them to query prod destination.")
            raise

        creds_info = None
        try:
            creds_info = dlt.secrets.get("destination.bigquery.credentials")
        except Exception:
            creds_info = None

        if not creds_info or not isinstance(creds_info, dict):
            logger.error("BigQuery credentials not found in dlt.secrets['destination.bigquery.credentials']. Cannot query prod destination.")
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