# Properties Feature — Step-by-step Execution Plan ✅

**Goal:** Add a new `properties` resource implemented as a file-per-resource, accessible through a separate entrypoint for daily runs. The resource must use `uid` as the primary key and use `merge` write disposition so running the daily job is idempotent.

**Status:** Implemented & tested. `hostfully_pipeline/resources/properties.py` and `hostfully_properties.py` have been added and utilities (`api_helpers`, `database`, and `report`) were updated to include properties support.

**Notes from latest edits:**
- `hostfully_properties_source` requires adding `agency_uid` parameter (from `dlt.config`) and the resource uses an incremental `updatedSince` parameter driven by `{incremental.start_value}`.
- The properties resource includes an `incremental` config (cursor_path `updatedUtcDateTime` with an initial value).
- `hostfully_properties.py` contains a `logging.basicConfig` call so logs are visible when running the entrypoint locally.
- `hostfully_properties_source` uses `"max_table_nesting": 1` in the resource config. to limit nested JSON expansion.
---

## 1) Summary & Assumptions 💡
- Properties are fetched from the Hostfully API single-resource endpoint (paged if necessary).
- The implementation will be a new file: `hostfully_pipeline/resources/properties.py` and a new entrypoint script: `hostfully_properties.py` (daily run).
- Properties table primary key: `uid` (string). Write disposition: `merge`.
- Feature will be disabled from the main (regular) pipeline by default; it will be run via the dedicated entrypoint.
- Add counters and reporting so `properties` appears in monitoring & `generate_pipeline_report()`. Properties will have its own separate report which shares the same structure and format as the leads report but is maintained completely separately.

---

## 2) Files to Create / Modify (high level) 🔧
1. Create `hostfully_pipeline/resources/properties.py` (new dlt source resource)
2. Create `hostfully_properties.py` (entrypoint + pipeline.run call for daily jobs)
3. Update `hostfully_pipeline/utils/api_helpers.py` — add `properties` to `API_CALL_COUNTERS` and helpers
4. Update `hostfully_pipeline/utils/database.py` — change `get_rows_count_from_db()` to return a **dict** mapping table names to counts (callers will use the parts they need)
5. Update `hostfully_pipeline/utils/report.py` — add logic to generate a separate `properties` report (same structure/format as leads report) and consume the dict of counts
6. Update `hostfully_pipeline/main.py` — add `run_properties_pipeline()` (not executed by default)
7. Update documentation: `documentation/PROPERTIES_FEATURE_PLAN.md` and this execution plan
8. Add/modify any exports (e.g., package `__init__` or CLI entrypoints) if project uses them

---

## 3) Detailed Implementation Steps (ordered) 🧭

### Step A — Create the properties resource (core logic)
1. Add `hostfully_pipeline/resources/properties.py` as a `@dlt.source` (following the existing `leads` resource pattern) configured for `GET /properties` (or the real endpoint path). Use `rest_api_resources` configuration so dlt's built-in pagination, rate-limit/backoff, and 404 handling are used (no custom pagination/backoff code required).
   - The source should accept `agency_uid` (from `dlt.config`) and pass it as `agencyUid` to the endpoint params.
   - The endpoint uses an incremental `updatedSince` parameter sourced from `{incremental.start_value}` with `cursor_path` `updatedUtcDateTime` and an appropriate `initial_value`.
   - Use `uid` as the primary key (declare in resource config).
   - Set write disposition to `merge` in pipeline or table config.
   - Add logging at INFO for pages and DEBUG for row details (entrypoint already includes `logging.basicConfig` for visibility).
   - Add increments to `API_CALL_COUNTERS` via `increment_api_counter('properties')` on each API call.

Estimated time: 30–60 minutes


### Step B — Add entrypoint for daily runs
1. Create `hostfully_properties.py` API entrypoint to run only the properties resource.
   - Keep it minimal: configure dlt pipeline and call `.run()` similarly to `hostfully_pipeline/main.py`.
   - Ensure it uses `merge` write disposition and the proper dataset name.
   - Add basic logging configuration (e.g., `logging.basicConfig`) in the entrypoint so logs are visible when run locally.

Estimated time: 20–40 minutes


### Step C — Update utils & reporting
1. Add `properties` key to `API_CALL_COUNTERS` in `hostfully_pipeline/utils/api_helpers.py` and update any helper functions.
2. Update `get_rows_count_from_db()` in `utils/database.py` to query `properties` table.
3. Update `generate_pipeline_report()` in `utils/report.py` to include `properties` in the summary and to read the `API_CALL_COUNTERS['properties']` count.

Estimated time: 20–40 minutes




### Step E — Documentation and housekeeping
1. Update `documentation/PROPERTIES_FEATURE_PLAN.md` and add release notes.
2. Run linter/formatter (flake8/black) and fix style issues.

Estimated time: 20–40 minutes


---

## 4) Acceptance Criteria (Merge-ready) 🎯
- Code passes linting/formatting checks
- `properties` table created with `uid` primary key and uses merge write
- `hostfully_properties.py` runs and writes the expected rows when executed
- `API_CALL_COUNTERS['properties']` increases with API calls and a separate `properties` report is generated that mirrors the structure/format of the leads report
- Documentation updated with usage instructions and run examples


If you'd like, I can apply these changes directly and open a draft PR for you to review — say the word and I'll implement the files. 🚀
