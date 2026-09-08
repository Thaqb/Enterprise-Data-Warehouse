# Hostfully Pipeline Enhancement - Implementation Plan

**Date:** January 22, 2026  
**Goal:** Enhance pipeline with fail-fast rate limiting, threads resource, and single-table messages strategy with automatic deduplication

---

## SECTION 1: Clean Up Legacy Code Artifacts

**Files to modify:**
- `enrichment/enrichers.py`
- `hostfully_pipeline.py`

**Changes:**

### 1.1 Remove undefined function calls in `enrichment/enrichers.py`
- **Line ~103-104**: Remove `global _first_rate_limit_recorded` and related try/except block
- **Line ~107**: Remove `import time as _time` (duplicate import)
- **Line ~110**: Remove `_add_time("enrichment_time_seconds", _time.time() - start)`
- **Line ~114-116**: Remove rate limit tracking block:
  ```python
  if not _first_rate_limit_recorded:
      rate_limit_first = response.headers.get("x-ratelimit-remaining")
      set_rate_limit_remaining_first(rate_limit_first)
      _first_rate_limit_recorded = True
  ```
- **Line ~121-122**: Remove:
  ```python
  rate_limit_last = response.headers.get("x-ratelimit-remaining")
  set_rate_limit_remaining_last(rate_limit_last)
  ```

### 1.2 Remove commented metrics calls
- `enrichment/enrichers.py` line ~111: Remove `# _incr("detail_api_calls", 1)`
- `hostfully_pipeline.py` line ~233: Remove `# _incr("total_messages_fetched", 1)`

**Expected outcome:** Clean code without undefined functions or commented-out metrics

---

## SECTION 2: Implement Fail-Fast Rate Limit Strategy

**Files to modify:**
- `hostfully_pipeline.py` (add exception class)
- `enrichment/enrichers.py` (update rate limit handling)
- `.dlt/config.toml` (add new config parameter)

**Changes:**

### 2.1 Create custom exception class in `hostfully_pipeline.py`
Add after imports, before `_get_session()` function:
```python
class RateLimitExceededError(Exception):
    """Raised when Hostfully API rate limit (429) is hit and fail_on_rate_limit is enabled."""
    pass
```

### 2.2 Add config parameter to `.dlt/config.toml`
Add to `[hostfully]` section:
```toml
# Fail-fast on rate limit (production) vs wait-and-retry (development)
fail_on_rate_limit = true
```

### 2.3 Update rate limit handling in `enrichment/enrichers.py`
**Location:** `fetch_detail_with_retry()` function, line ~145-160 (429 handling block)

**Replace:**
```python
elif response.status_code == 429:
    logger.error(...)
    # Log rate limit headers if available
    if 'x-ratelimit-remaining' in response.headers:
        logger.error(...)
    # Wait longer for rate limit (1 minute as compromise, API suggests 1 hour)
    if attempt < rule.retry_attempts:
        logger.warning(f"Waiting 60 seconds before retry due to rate limit...")
        time.sleep(60)
```

**With:**
```python
elif response.status_code == 429:
    logger.error(
        f"Rate limit (429) hit for {rule.endpoint_name} UID {uid}. "
        f"Headers: {response.headers}"
    )
    # Check config: fail-fast or wait-and-retry
    try:
        cfg = dlt.config.get("hostfully", {}) or {}
    except Exception:
        cfg = {}
    fail_on_rate_limit = cfg.get("fail_on_rate_limit", True)
    
    if fail_on_rate_limit:
        raise RateLimitExceededError(
            f"Hostfully rate limit exceeded. Remaining: {response.headers.get('x-ratelimit-remaining')}. "
            f"Reschedule pipeline after rate limit window."
        )
    else:
        # Dev mode: wait and retry
        logger.warning(f"Waiting 60 seconds before retry due to rate limit...")
        if attempt < rule.retry_attempts:
            time.sleep(60)
```

### 2.4 Import RateLimitExceededError in `enrichment/enrichers.py`
Add at top of file after imports:
```python
# Import custom exceptions from main pipeline
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from hostfully_pipeline import RateLimitExceededError
```

### 2.5 Update rate limit handling in `hostfully_pipeline.py`
**Location:** `fetch_messages_for_lead()` function, line ~210-222 (429 handling block)

**Replace:**
```python
if response.status_code == 429:
    logger.warning(f"Rate limited when fetching messages for lead {lead_uid}. Headers: {response.headers}")
    if retries_remaining > 0:
        logger.warning(f"Sleeping {pause_seconds} seconds due to rate limit, retries remaining: {retries_remaining}")
        time.sleep(pause_seconds)
        retries_remaining -= 1
        continue
    else:
        logger.error(f"Rate limit exhausted for lead {lead_uid}. Skipping messages for this lead.")
        break
```

**With:**
```python
if response.status_code == 429:
    logger.error(f"Rate limit (429) hit fetching messages for lead {lead_uid}. Headers: {response.headers}")
    fail_on_rate_limit = cfg.get("fail_on_rate_limit", True)
    
    if fail_on_rate_limit:
        raise RateLimitExceededError(
            f"Hostfully rate limit exceeded. Remaining: {response.headers.get('x-ratelimit-remaining')}. "
            f"Reschedule pipeline after rate limit window."
        )
    else:
        # Dev mode: wait and retry
        if retries_remaining > 0:
            logger.warning(f"Sleeping {pause_seconds} seconds, retries remaining: {retries_remaining}")
            time.sleep(pause_seconds)
            retries_remaining -= 1
            continue
        else:
            logger.error(f"Rate limit exhausted for lead {lead_uid}. Skipping messages.")
            break
```

**Expected outcome:** Pipeline fails fast on 429 in production, logs rate limit info clearly

---

## SECTION 3: Add Threads Resource with Client-Side Incremental Filtering

**Files to modify:**
- `hostfully_pipeline.py`

**Changes:**

### 3.1 Create `threads_incremental()` resource function
**Location:** Add after `lead_uids_from_db()` function, before `fetch_messages_for_lead()` transformer

**Implementation:**
```python
@dlt.resource(
    name="threads",
    write_disposition="merge",
    primary_key="uid",
)
def threads_incremental(
    api_key: str = dlt.secrets.value,
    agency_uid: str = dlt.config.value,
    last_sync_date: dlt.sources.incremental[str] = dlt.sources.incremental(
        "lastUpdateDate",
        initial_value="2020-01-01T00:00:00Z"
    )
) -> Iterator[Dict[str, Any]]:
    """Fetch threads with client-side incremental filtering.
    
    Since threads endpoint doesn't support updatedSince parameter, we fetch
    pages in DESC order (by lastUpdateDate) and stop when we hit old threads.
    
    Only yields threads with LEAD participant (skips legacy GUEST threads).
    
    Args:
        api_key: Hostfully API key
        agency_uid: Agency UID to filter threads
        last_sync_date: dlt incremental state (last run's max lastUpdateDate)
        
    Yields:
        Thread records with lastUpdateDate > last_sync_date AND has LEAD participant
    """
    base_url = "https://api.hostfully.com/api/v3.2/threads"
    session = _get_session(api_key)
    
    # Rate limit config
    try:
        cfg = dlt.config.get("hostfully", {}) or {}
    except Exception:
        cfg = {}
    fail_on_rate_limit = cfg.get("fail_on_rate_limit", True)
    
    params = {
        "_limit": 1000,
        "agencyUid": agency_uid
    }
    
    cursor = None
    total_fetched = 0
    total_yielded = 0
    stopped_early = False
    
    logger.info(f"Fetching threads updated since {last_sync_date.start_value}")
    
    while True:
        if cursor:
            params["_cursor"] = cursor
        
        try:
            response = session.get(base_url, params=params, timeout=30)
            
            # Handle 429 with fail-fast
            if response.status_code == 429:
                logger.error(f"Rate limit hit fetching threads. Headers: {response.headers}")
                if fail_on_rate_limit:
                    raise RateLimitExceededError("Hostfully rate limit exceeded")
                else:
                    logger.warning("Waiting 3600s due to rate limit...")
                    time.sleep(3600)
                    continue
            
            response.raise_for_status()
            data = response.json()
            threads = data.get("threads", [])
            
            if not threads:
                logger.info("No more threads to fetch")
                break
            
            # Client-side incremental filtering
            for thread in threads:
                total_fetched += 1
                thread_update_date = thread.get("lastUpdateDate", "")
                
                # Check if thread is newer than last sync
                if thread_update_date <= last_sync_date.start_value:
                    # Thread is old, stop fetching more pages
                    logger.info(
                        f"Reached thread {thread['uid']} with lastUpdateDate {thread_update_date} "
                        f"<= last sync {last_sync_date.start_value}. Stopping early."
                    )
                    stopped_early = True
                    break
                
                # Only yield threads with LEAD participant (skip legacy GUEST threads)
                has_lead_participant = any(
                    p.get("participantType") == "LEAD"
                    for p in thread.get("participants", [])
                )
                
                if has_lead_participant:
                    yield thread
                    total_yielded += 1
                else:
                    logger.debug(f"Skipping legacy thread {thread['uid']} (no LEAD participant)")
            
            # Stop pagination if we hit old threads
            if stopped_early:
                break
            
            # Get next cursor
            cursor = data.get("_paging", {}).get("_nextCursor")
            if not cursor:
                logger.info("No more pages (no _nextCursor)")
                break
            
            # Log progress every page
            logger.info(
                f"Threads progress: fetched {total_fetched}, yielded {total_yielded} "
                f"(with LEAD participant)"
            )
        
        except RateLimitExceededError:
            raise  # Re-raise to stop pipeline
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching threads: {e}")
            raise
    
    logger.info(
        f"Threads sync complete: {total_yielded} threads yielded out of {total_fetched} fetched. "
        f"Early stop: {stopped_early}"
    )
```

**Key design decisions:**
- **Client-side incremental:** Compare `lastUpdateDate` with `last_sync_date.start_value` because API doesn't support `updatedSince`
- **Early stop:** Leverage DESC sorting to stop when hitting old threads (optimization)
- **LEAD-only filter:** Only yield threads with `participantType == "LEAD"` (skips legacy GUEST threads)
- **No NULL leadUid:** This ensures all threads in v2 can be linked to leads

**Expected outcome:** Threads table with only modern threads (LEAD participants), incremental state tracked

---

## SECTION 4: Add Messages from Threads Transformer

**Files to modify:**
- `hostfully_pipeline.py`

**Changes:**

### 4.1 Create `fetch_messages_from_thread()` transformer function
**Location:** Add after `fetch_messages_for_lead()` transformer

**Implementation:**
```python
@dlt.transformer(
    name="messages_from_threads",
    write_disposition="merge",
    primary_key=["uid", "threadUid", "leadUid"],
    parallelized=True
)
def fetch_messages_from_thread(
    thread_record: Dict[str, Any],
    api_key: str = dlt.secrets.value,
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
    base_url = "https://api.hostfully.com/api/v3.2/messages"
    params = {"_limit": 1000, "threadUid": thread_uid}
    cursor = None
    
    # Rate limit config
    try:
        cfg = dlt.config.get("hostfully", {}) or {}
    except Exception:
        cfg = {}
    fail_on_rate_limit = cfg.get("fail_on_rate_limit", True)
    
    message_count = 0
    
    while True:
        if cursor:
            params["_cursor"] = cursor
        
        try:
            response = session.get(base_url, params=params, timeout=30)
            
            # Handle 404 (thread has no messages) - normal, skip silently
            if response.status_code == 404:
                logger.debug(f"Thread {thread_uid} has no messages (404)")
                break
            
            # Handle 429 with fail-fast
            if response.status_code == 429:
                logger.error(
                    f"Rate limit hit fetching messages for thread {thread_uid}. "
                    f"Headers: {response.headers}"
                )
                if fail_on_rate_limit:
                    raise RateLimitExceededError("Hostfully rate limit exceeded")
                else:
                    logger.warning("Waiting 3600s due to rate limit...")
                    time.sleep(3600)
                    continue
            
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
        
        except RateLimitExceededError:
            raise  # Re-raise to stop pipeline
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching messages for thread {thread_uid}: {e}")
            break
    
    logger.debug(f"Fetched {message_count} messages for thread {thread_uid} (leadUid: {lead_uid})")
```

**Key design decisions:**
- **threadUid already in response:** No need to inject it (API provides it)
- **Only inject leadUid:** Extract from `participants` array and add to each message
- **No NULL leadUid:** Upstream filter ensures only LEAD threads processed
- **Composite key:** `["uid", "threadUid", "leadUid"]` - all fields guaranteed present
- **Empty messages handling:** Skip threads with no messages (200 with empty array)

**Expected outcome:** Messages written to same table as leadUid-based messages, automatic deduplication via merge disposition

---

## SECTION 5: Implement Single-Table Pipeline Logic with First-Run Detection

**Files to modify:**
- `hostfully_pipeline.py` (main execution block)

**Changes:**

### 5.1 Add first-run detection helper function
**Location:** Add before `if __name__ == "__main__":` block

```python
def is_first_messages_run(pipeline_name: str) -> bool:
    """Check if this is the first run for messages (messages table doesn't exist).
    
    Args:
        pipeline_name: Name of the dlt pipeline
        
    Returns:
        True if messages table doesn't exist (first run), False otherwise
    """
    db_path = f"{pipeline_name}.duckdb"
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'hostfully_pipeline_dataset' "
            "AND table_name = 'messages'"
        ).fetchone()
        conn.close()
        return (result[0] == 0)
    except Exception as e:
        logger.warning(f"Could not check for messages table: {e}. Assuming first run.")
        return True
```

### 5.2 Update main execution block
**Location:** Replace entire `if __name__ == "__main__":` block

**New implementation:**
```python
if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Starting Hostfully pipeline with enrichment...")
    logger.info("=" * 80)
    
    # Get API credentials
    api_key = dlt.secrets.get("api_key")
    
    # -------------------------------------------------------------------------
    # STAGE 1: Leads with Enrichment (Always Run)
    # -------------------------------------------------------------------------
    logger.info("\n[STAGE 1] Fetching and enriching leads...")
    
    leads_rule = HOSTFULLY_ENRICHMENT_CONFIG.rules.get("leads")
    source = hostfully_rest_api_source()
    
    enrichment_transformer = create_enrichment_transformer(
        rule=leads_rule,
        base_url="https://api.hostfully.com/api/v3.2/",
        headers={"X-HOSTFULLY-APIKEY": api_key},
    )
    
    enriched_leads = source.resources["leads"] | enrichment_transformer
    
    load_info = pipeline.run(enriched_leads)
    logger.info(f"[STAGE 1] Leads completed. Load info:\n{load_info}")
    
    # -------------------------------------------------------------------------
    # STAGE 2: Messages - Single-Table Strategy (First Run vs Subsequent Runs)
    # -------------------------------------------------------------------------
    
    # Detect if this is first run
    first_run = is_first_messages_run(pipeline.pipeline_name)
    
    if first_run:
        # ---------------------------------------------------------------------
        # FIRST RUN: Fetch messages via leadUid → messages table
        # ---------------------------------------------------------------------
        logger.info("\n[STAGE 2a] FIRST RUN detected - Fetching all messages via leadUid...")
        logger.info("This will populate messages table with historical baseline.")
        
        messages_from_leads = (
            lead_uids_from_db(pipeline_name=pipeline.pipeline_name) 
            | fetch_messages_for_lead
        )
        
        load_info = pipeline.run(messages_from_leads, table_name="messages")
        logger.info(f"[STAGE 2a] Messages baseline completed. Load info:\n{load_info}")
        logger.info(
            "\nNEXT RUN: Pipeline will fetch threads and messages via threadUid → same messages table"
        )
    
    else:
        # ---------------------------------------------------------------------
        # SUBSEQUENT RUNS: Fetch threads + messages via threadUid → messages table
        # ---------------------------------------------------------------------
        logger.info("\n[STAGE 2b] SUBSEQUENT RUN - Fetching threads and messages via threadUid...")
        
        # Fetch threads (incremental, LEAD-only)
        threads = threads_incremental()
        load_info = pipeline.run(threads)
        logger.info(f"[STAGE 2b.1] Threads completed. Load info:\n{load_info}")
        
        # Fetch messages for updated threads → SAME messages table
        threads_source = threads_incremental()
        messages_from_threads = (
            threads_source 
            | fetch_messages_from_thread
        )
        
        load_info = pipeline.run(messages_from_threads, table_name="messages")
        logger.info(f"[STAGE 2b.2] Messages incremental completed. Load info:\n{load_info}")
        logger.info("dlt merge disposition automatically deduplicates on composite key.")
    
    # -------------------------------------------------------------------------
    # Pipeline Complete
    # -------------------------------------------------------------------------
    logger.info("\n" + "=" * 80)
    logger.info("Hostfully pipeline completed successfully!")
    logger.info("=" * 80)
    
    print(load_info)  # noqa: T201
```

**Key design decisions:**
- **Stage 1 (Always):** Leads enrichment runs every time
- **Stage 2a (First Run):** Messages via leadUid → `messages` table (baseline)
- **Stage 2b (Subsequent):** Threads + messages via threadUid → SAME `messages` table (incremental)
- **Single table:** Both sources merge into one table, dlt handles deduplication automatically
- **Clear logging:** Shows which stage/mode is running
- **No manual union needed:** dlt merge disposition handles everything

**Expected outcome:** Single messages table with automatic deduplication, no BigQuery union required

---

## SECTION 6: Document Messages Table Strategy

**Files to modify:**
- `DOCUMENTATION.md`

**Changes:**

### 6.1 Add new section after "How to Use the Pipeline"
**Section title:** "Messages Table Architecture"

**Content:**
```markdown
## Messages Table Architecture

### Single-Table Strategy with Automatic Deduplication

The messages endpoint doesn't support incremental filtering (`createdSince` parameter is broken). To avoid refetching all messages every run while maintaining a single source of truth, we use a **single-table strategy with dlt's automatic deduplication**.

### How It Works

**First Run:**
- Fetches ALL messages via `leadUid` for all leads in database
- Populates `messages` table with historical baseline
- Uses merge disposition with composite primary key: `["uid", "threadUid", "leadUid"]`

**Subsequent Runs:**
- Fetches threads updated since last run (incremental)
- Fetches messages for those threads via `threadUid`
- Writes to SAME `messages` table
- dlt merge disposition automatically deduplicates on composite key

### Schema

**Table:** `messages`

**Primary Key:** `["uid", "threadUid", "leadUid"]` (composite)

**Write Disposition:** `merge` (automatic deduplication)

**Key Fields:**
- `uid` - Unique message identifier
- `threadUid` - Thread identifier (provided by API)
- `leadUid` - Lead identifier (injected by pipeline)
- `createdUtcDateTime` - Message creation timestamp
- `status` - Message status (CREATED, SENT, etc.)
- `type` - Message type (AIRBNB, EMAIL, etc.)
- `senderType` - Sender type (AGENCY, GUEST)
- `content` - Message content (subject, text)
- `attachments` - Message attachments array

### Data Guarantees

- **No NULL leadUid:** Guaranteed in all scenarios
  - First run: leadUid is the query parameter
  - Subsequent runs: Only threads with LEAD participant are processed
- **No Duplicates:** dlt merge disposition ensures one row per composite key
- **Complete History:** First run establishes baseline, subsequent runs add incremental updates
- **Automatic Deduplication:** If same message appears in both leadUid and threadUid fetches, only one copy is kept

### BigQuery Migration

No special setup required! The single `messages` table works identically in DuckDB and BigQuery.

Just update `.dlt/config.toml` destination to BigQuery and run the pipeline.

### Query Examples

```sql
-- Count messages per lead
SELECT leadUid, COUNT(*) as message_count
FROM messages
GROUP BY leadUid
ORDER BY message_count DESC;

-- Find recent messages
SELECT uid, threadUid, leadUid, createdUtcDateTime, content
FROM messages
ORDER BY createdUtcDateTime DESC
LIMIT 100;

-- Find unread threads (join with threads table)
SELECT DISTINCT m.threadUid, m.leadUid, COUNT(*) as message_count
FROM messages m
JOIN threads t ON m.threadUid = t.uid
WHERE t.participantsReadStatuses LIKE '%UNREAD%'
GROUP BY m.threadUid, m.leadUid;
```
```

**Expected outcome:** Clear documentation explaining single-table strategy and data guarantees

---

## SECTION 7: Testing Strategy

### Test Plan

**Test Environment:** DuckDB (development)

#### Test 1: First Run (Clean State)
1. Delete `.dlt/` directory (clear state)
2. Delete `hostfully_pipeline.duckdb` (clear data)
3. Run pipeline: `python hostfully_pipeline.py`
4. **Expected:**
   - Stage 1: Leads enrichment runs
   - Stage 2a: Messages table created (via leadUid)
   - No threads table yet
5. **Verify:**
   - `SELECT COUNT(*) FROM messages` > 0
   - All rows have `leadUid` populated
   - Composite key `(uid, threadUid, leadUid)` is unique
   - Check sample: `SELECT uid, threadUid, leadUid FROM messages LIMIT 10`

#### Test 2: Second Run (Incremental)
1. Wait a few minutes (or update a thread via Hostfully UI)
2. Run pipeline again: `python hostfully_pipeline.py`
3. **Expected:**
   - Stage 1: Leads enrichment runs (incremental)
   - Stage 2b: Threads fetched (incremental), messages appended to SAME messages table (via threadUid)
   - Original messages preserved (no refetch)
4. **Verify:**
   - `SELECT COUNT(*) FROM threads` > 0
   - `SELECT COUNT(*) FROM messages` >= previous count (new messages added)
   - All threads have LEAD participant (no GUEST)
   - All messages rows have `leadUid` populated
   - No duplicates: `SELECT uid, threadUid, leadUid, COUNT(*) FROM messages GROUP BY uid, threadUid, leadUid HAVING COUNT(*) > 1` returns 0 rows

#### Test 3: Rate Limit Handling (429)
1. Set `fail_on_rate_limit = true` in config
2. Trigger rate limit (run pipeline rapidly or use test account with low limit)
3. **Expected:**
   - Pipeline raises `RateLimitExceededError`
   - Clear error message with rate limit headers logged
   - Pipeline exits immediately (no 1-hour wait)
4. **Verify:**
   - Error message shows remaining calls and reset time
   - No partial data loaded (dlt should rollback)

#### Test 4: Deduplication Verification (DuckDB)
1. After Test 2, verify no duplicates in messages table:
   ```sql
   -- Check for duplicate composite keys
   SELECT uid, threadUid, leadUid, COUNT(*) as duplicate_count
   FROM messages
   GROUP BY uid, threadUid, leadUid
   HAVING COUNT(*) > 1;
   ```
2. **Expected:**
   - Query returns 0 rows (no duplicates)
   - dlt merge disposition successfully deduplicates
3. **Additional check:**
   ```sql
   -- Verify all messages have leadUid populated
   SELECT COUNT(*) FROM messages WHERE leadUid IS NULL;
   ```
   Should return 0

#### Test 5: Legacy Thread Filtering
1. Check if agency has legacy threads:
   ```bash
   curl "https://api.hostfully.com/api/v3.2/threads?agencyUid={uid}&_limit=100" \
     -H "X-HOSTFULLY-APIKEY: {key}"
   ```
2. Look for threads with `participantType: "GUEST"`
3. **Expected:**
   - Legacy threads NOT yielded by `threads_incremental()`
   - No messages fetched for legacy threads
   - Log message: "Skipping legacy thread {uid} (no LEAD participant)"

---

## SECTION 8: Rollout Plan

### Phase 1: Development Testing (DuckDB)
- [ ] Implement all changes (Sections 1-6)
- [ ] Run Test 1-5 (see Section 7)
- [ ] Fix any issues
- [ ] Verify data quality

### Phase 2: Staging (DuckDB → BigQuery)
- [ ] Update `.dlt/config.toml` to use BigQuery destination
- [ ] Run pipeline first time (messages table created with baseline)
- [ ] Run pipeline second time (threads fetched, messages appended incrementally)
- [ ] Verify data in BigQuery (no union needed, single table)
- [ ] Verify no duplicates using composite key query

### Phase 3: Production Deployment
- [ ] Schedule pipeline (Airflow/Prefect/cron)
- [ ] Set `fail_on_rate_limit = true` in production config
- [ ] Set up monitoring/alerting on pipeline failures (handled in Airflow)
- [ ] No BigQuery union refresh needed (single table, automatic deduplication)

### Phase 4: Add More Endpoints (Future)
- [ ] Properties endpoint (with enrichment if needed)
- [ ] Orders endpoint
- [ ] Transactions endpoint
- [ ] Follow same pattern: incremental loading, fail-fast rate limiting

---

## DECISIONS CONFIRMED

### Decision 1: Threads Table Retention ✅
**Keep threads table in BigQuery** (useful for analytics on thread metadata)
- Thread metadata includes: `creationDate`, `lastUpdateDate`, `participants`, `participantsReadStatuses`
- Useful for thread-level analytics (e.g., unread threads, participant activity)
- Table will be loaded alongside messages table

### Decision 2: Single-Table Strategy ✅
**Use single messages table with automatic dlt merge deduplication**
- No manual BigQuery union needed
- Simpler architecture, leverages dlt native capabilities
- First run detection ensures baseline load, subsequent runs append incrementally
- No concerns about data loss from overlapping fetches

### Decision 3: Rate Limit Monitoring ✅
**No pipeline-level monitoring (rely on fail-fast error handling)**
- Rate limit monitoring/alerting handled in orchestration layer (Airflow)
- Pipeline raises `RateLimitExceededError` on 429, Airflow handles retry/alerting
- Simpler pipeline code, separation of concerns

### Decision 4: Implementation Approach ✅
**Phased implementation with review between phases**
- **Phase 1:** Sections 1-2 (cleanup + fail-fast rate limiting)
- **Phase 2:** Sections 3-5 (threads + messages + pipeline logic)
- Review after each phase before proceeding

---

## SUMMARY

**Architecture:**
- **Single-table messages strategy** with automatic dlt merge deduplication
- First run: Baseline load via leadUid → messages table
- Subsequent runs: Incremental updates via threadUid → SAME messages table
- No manual union needed, dlt handles everything

**Total changes:**
- 4 files modified: `enrichers.py`, `hostfully_pipeline.py`, `config.toml`, `DOCUMENTATION.md`
- 3 new functions: `threads_incremental()`, `fetch_messages_from_thread()`, `is_first_messages_run()`
- 1 new exception: `RateLimitExceededError`
- 1 new config param: `fail_on_rate_limit`
- 1 new empty messages handling: Skip threads with no messages (200 with empty array)

**Key improvements:**
- ✅ Cleaner architecture (single table vs two tables)
- ✅ Leverages dlt native merge deduplication
- ✅ No BigQuery union complexity
- ✅ Fail-fast rate limiting for production
- ✅ Client-side incremental filtering for threads
- ✅ Legacy thread filtering (LEAD-only)

**Estimated implementation time:**
- Phase 1 (Sections 1-2): 1 hour
- Phase 2 (Sections 3-5): 2 hours  
- Testing: 1-2 hours  
- **Total:** 4-5 hours

**Ready for phased implementation!**
