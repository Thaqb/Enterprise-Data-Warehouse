# Property Calendar Feature — Implementation Plan

**Goal:** Add a daily-updated Property Calendar stream that fetches availability/pricing day-by-day for each property and writes a single flattened table `property_calendar` with composite primary key `("propertyUid","date")` and `merge` write disposition.

**Status:** Draft — ready for your review. This plan follows repository patterns (file-per-resource, transformer-based concurrent fetch) and your explicit constraints.

---

## Design summary (decisions)
- Driver for calendar fetch: **property UIDs are read from DB** table `hostfully_pipeline_dataset.properties` (all UIDs every run). This ensures daily refresh for all properties.
- Entrypoint: **reuse** `hostfully_properties.py` and add a dedicated function `run_property_calendar_pipeline()` (no separate entrypoint file). The function will be run by scheduler (Airflow/cron).
- Backfill configuration: use `dlt.config` keys under `hostfully.property_calendar.*` (see keys below). Initial backfill values: `backfill_from=2023-01-01`, `backfill_to=2025-12-31`.
- Daily behavior: perform a per-property incremental fetch using a last-synced date saved per property in the pipeline state; for each property `from` = its last-synced date (inclusive) and `to` = today's date. After a successful fetch update that property's last-synced date to today's date. The initial backfill run should set each property's last-synced date to the configured `backfill_to` (e.g., 2025-12-31).
- Concurrency: `ThreadPoolExecutor` with `max_workers` default 20 (configurable via `hostfully.property_calendar.max_workers`).
- API behavior: calendar endpoint is single-page (no paginator). The request must include `propertyUid`, `from`, and `to`. The server rejects requests for ranges > 3 years (HTTP 400); transformer validates windows.
- Error handling: **on 429** log an error and raise for the response status so the pipeline stops immediately. For 400 range errors (range > 3 years), log and raise to force correction of backfill params. For network errors/timeouts, log and skip that property so transient network issues do not stop the entire run.
- Metrics & reporting: add `"property_calendar"` to `API_CALL_COUNTERS` and surface the count in `generate_pipeline_report()`. You are not interested in pre/post row counts for `property_calendar` — **do NOT** add row-count queries for this table to `get_rows_count_from_db()` or the report.
- Data model: a single flattened table `property_calendar` only (no nested tables). No raw JSON column (explicitly requested).

---

## Config keys (proposed)
Add under `[hostfully]` in `.dlt/config.toml`:
- `property_calendar.backfill_from` = "2023-01-01"  # initial backfill start date
- `property_calendar.backfill_to` = "2025-12-31"    # initial backfill end date
- `property_calendar.max_workers` = 20

Notes: Per-property last-synced dates are maintained in the pipeline state (source- or pipeline-scoped state) and used as the `from` parameter on subsequent runs. The `backfill_from`/`backfill_to` keys are used only for the initial backfill.

---

## Files to add / modify (exact list)
- New: `hostfully_pipeline/resources/property_calendar.py`
  - Symbols/functions:
    - `property_uids_from_db(pipeline_name: str) -> Iterator[str]` — yields property UIDs read from DuckDB `hostfully_pipeline_dataset.properties` table in batches.
    - `property_calendar_transformer_factory()` → returns a `@dlt.transformer` named `raw_property_calendar` that accepts an iterator of UIDs and yields flattened calendar rows marked to `raw_property_calendar` table.
  - Implementation details in file: thread-local session, worker pool, per-property fetch helper (`_fetch_property_calendar`), date-window validation.

- Modify: `hostfully_properties.py`
  - Add `run_property_calendar_pipeline()` (not executed by default). It will:
    - Resolve `dlt.config` keys for backfill vs daily run
    - Create `pipeline = dlt.pipeline(...)` (reuse pipeline name/destination)
    - Get property UIDs via `property_uids_from_db(pipeline_name=pipeline.pipeline_name)`
    - Compute `from`/`to` dates for requests (backfill or daily window)
    - Create transformer via `property_calendar_transformer_factory(api_key=api_key)`
    - Run pipeline: `pipeline.run(property_uids | property_calendar_transformer, table_name="property_calendar", write_disposition="merge")`
    - Track and log API counts and raise on 429

- Modify: `hostfully_pipeline/utils/api_helpers.py`
  - Add key `"property_calendar": 0` to `API_CALL_COUNTERS` and ensure `increment_api_counter('property_calendar')` is called for each calendar request.

- Modify: `hostfully_pipeline/utils/report.py`
  - Include `API_CALL_COUNTERS['property_calendar']` in the API Calls breakdown section (no DB row diffs for property_calendar).

- Documentation: `documentation/PROPERTY_CALENDAR_FEATURE_PLAN.md` (this file) — add run instructions, config notes, and backfill steps.

> Tests: you said you will handle tests — the plan will not add tests; we will not include test files.

---

## Data schema (single flattened table)
- Table: `property_calendar`
- Columns (suggested minimal set):
  - `propertyUid` (string)  — part of PK
  - `date` (date)          — part of PK (calendar entry date)
  - `price_value` (decimal, nullable)
  - `price_currency` (string, nullable)
  - `unavailable` (boolean)
  - `unavailability_reason` (string, nullable)
  - `available_for_check_in` (boolean)
  - `available_for_check_out` (boolean)
  - `minimum_stay_length` (int, nullable)
  - `maximum_stay_length` (int, nullable)
  - `notes` (string, nullable)
- Primary key: composite `("propertyUid","date")`
- Write disposition: `merge`
- Note: **No `raw` JSON column** per your instruction.

---

## Implementation details — transformer behavior
- Transformer receives an iterator of property UIDs (batched or singular UIDs). For performance, it will operate per page/batch of UIDs: e.g., `for batch in batches(property_uids, batch_size=...):` submit each property fetch in `ThreadPoolExecutor(max_workers=config_max_workers)`.
- `_fetch_property_calendar(property_uid, from_date, to_date, base_url, api_key)` does:
  - Build request params: `from`, `to` and path `property-calendar/{propertyUid}`
  - Make GET request with session timeout
  - Call `increment_api_counter('property_calendar')`
  - If response.status_code == 429: log and raise `RateLimitExceededError` to stop pipeline (per your instruction)
  - If response.status_code == 400 and server message contains the range error, log and raise to force correction
  - On 2xx: parse `calendar.entries` and yield flattened rows (one per entry) with `date` and parsed fields
  - On network error/timeout: log a warning and skip that property
- Transformer yields rows using `dlt.mark.with_table_name(row, 'property_calendar')` so normalization writes into `property_calendar` table with `merge` deduping.

---

## Backfill & daily run procedure
1. Configure initial backfill in `.dlt/config.toml` using `hostfully.property_calendar.backfill_from` and `hostfully.property_calendar.backfill_to`.
2. Run once: execute `run_property_calendar_pipeline()` to perform the full backfill for all properties (from `backfill_from` to `backfill_to`). On success, the pipeline sets each property's last-synced date in state to `backfill_to` (e.g., 2025-12-31).
3. Daily scheduled run (Airflow/cron): `run_property_calendar_pipeline()` reads the last-synced date for each property from the pipeline state (or uses `backfill_to` if none), sets `from` to that value and `to` to today's date, fetches the calendar for that property, and on success updates that property's last-synced date to today's date. If the fetch for a property fails, its last-synced state is not updated so the next run will retry and fill gaps.
---

## Observability & runbook
- Logs: include per-property INFO/DEBUG lines when a calendar is fetched, and ERROR lines on 429 (pipeline will stop) or 400.
- Report: `generate_pipeline_report()` will include `API_CALL_COUNTERS['property_calendar']` in the API CALLS section so daily totals are visible.
- Operations: Start with `max_workers=20`; after a few runs, review logs and API call patterns, then increase/decrease as needed.

---

## Acceptance criteria (for merging implementation)
- `hostfully_pipeline/resources/property_calendar.py` added and contains `property_uids_from_db` and `property_calendar_transformer_factory` symbols.
- `hostfully_properties.py` includes `run_property_calendar_pipeline()` and uses `dlt.config` keys for initial backfill parameters and `max_workers`.
- Per-property last-synced dates are stored in the pipeline state after backfill and updated after each successful per-property run; these stored dates are used as the `from` parameter for subsequent runs.
- `API_CALL_COUNTERS` contains `"property_calendar"` and the report shows its daily total in `generate_pipeline_report()` (no DB row diffs required for this table).
- `property_calendar` table is created with composite PK (`propertyUid`,`date`) and `merge` write_disposition; rows have the flattened columns shown above (no `raw` JSON column).
- On 429, the pipeline logs an error and raises to stop immediately; on 400 range error, the pipeline logs and raises to force correction of backfill params.
---

## Next steps (if you approve)
- I will save this document to `documentation/PROPERTY_CALENDAR_FEATURE_PLAN.md` (requested now). After you review and accept it, I will prepare a concrete file-by-file change checklist (exact diffs) and then implement the changes only after you confirm.

---

If you want any wording or technical detail changed (e.g., column names, config key names, batch size policy), tell me which lines to adjust and I will update the plan before saving the file.
