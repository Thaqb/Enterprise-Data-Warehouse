# Orders Feature Implementation Plan

## Overview
Orders are added as a **child resource** of leads using dlt's REST API parent-child relationship pattern. Orders will be fetched in **Stage 1** alongside leads enrichment, loaded together in a **single atomic `pipeline.run()` call**.

## Key Characteristics
- **1:1 Relationship**: One order per lead
- **Incremental Behavior**: Inherits from leads (same `updatedUtcDateTime`)
- **No Pagination**: Single order per lead fetch
- **Write Mode**: Merge disposition on composite key `["uid", "leadUid"]`
- **Atomic Loading**: Leads + Orders load together in one transaction

## Stage 1 Execution Flow
```
Fetch leads (incremental, cursor pagination)
  ↓
Apply enrichment transformer (incomplete fields)
  ↓
For each lead, fetch corresponding order (parent-child auto-iteration)
  ↓
pipeline.run([enriched_leads, orders])
  ├─ Write enriched leads to enrich_leads table
  ├─ Write orders to orders table
  └─ Atomic transaction
```

## Implementation Steps

### Step 1: Update REST API Source
**File**: `hostfully_pipeline/resources/leads.py`

Add orders resource to REST API config with parent-child relationship:
- Parent: `leads`
- Endpoint: `/orders`
- Parameter: `leadUid: "{parent[uid]}"` (dlt substitutes parent UID)
- Data selector: `orders`
- Primary key: `["uid", "leadUid"]` (composite)
- Nested fields as JSON: `rent`, `fees`, `services`, `adjustments`

### Step 2: Update Pipeline Orchestration
**File**: `hostfully_pipeline/main.py`

- Extract orders resource: `orders = source.resources["orders"]`
- Update `pipeline.run()` to load both: `pipeline.run([enriched_leads, orders], ...)`
- Set primary keys: `primary_key={"enrich_leads": "uid", "orders": ["uid", "leadUid"]}`
- Capture trace: `stage1_trace = pipeline.last_trace` (covers both)

### Step 3: Update Database Utilities
**File**: `hostfully_pipeline/utils/database.py`

- Add query for orders count
- Update `get_rows_count_from_db()` return to include orders
- Handle missing orders table (first run)

### Step 4: Update API Tracking
**File**: `hostfully_pipeline/utils/api_helpers.py`

- Add `"orders"` to `API_CALL_COUNTERS` dictionary

### Step 5: Update Report Generation
**File**: `hostfully_pipeline/utils/report.py`

- Add `orders_count_before` and `orders_count_after` parameters
- Calculate: `orders_net_new = orders_count_after - orders_count_before`
- Extract from trace: `stage1_trace.last_normalize_info.row_counts.get('orders', 0)`
- Add orders section: "Orders (inserted)" with count
- Add to API breakdown

### Step 6: Update main.py Report Call
**File**: `hostfully_pipeline/main.py`

- Unpack orders counts from `get_rows_count_from_db()`
- Pass to `generate_pipeline_report()` with new parameters

## Edge Cases
1. **Lead with no order**: Empty `orders` array returned, no row inserted
2. **Order updates with lead**: Both have same `updatedUtcDateTime`, fetched together
3. **404 response**: Logged as info message, execution continues
4. **First run**: All leads and their orders fetched via initial cursor value

## Report Output
```
DATA LOADED THIS RUN:
  Orders (inserted)                                            X

TOTAL DATA IN DATABASE:
  Orders                                                       Y

API CALLS BREAKDOWN:
  - Orders                                                     0 (internal to dlt)
```

## Data Consistency
- Atomic transaction: Leads and orders commit together
- Merge disposition: Handles updates idempotently
- Deduplication: Composite key prevents duplicate orders
