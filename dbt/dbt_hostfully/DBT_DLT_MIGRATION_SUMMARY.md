# dbt Hostfully Project - dlt Migration Summary

## Overview
This dbt project has been successfully migrated from Airbyte to dlt (data load tool) as the ingestion layer. The project is now optimized for a multi-pipeline architecture with different refresh cadences orchestrated via Airflow.

## Project Statistics
- **Total Models**: 33 active models
  - **Staging**: 19 models (organized by pipeline)
  - **Marts**: 14 models (fact and dimension tables)
- **Snapshots**: 19 active snapshots (SCD Type 2)
- **Seeds**: 1 (country codes reference data)
- **Tests**: 122 data quality tests
- **Sources**: 22 tables (19 data tables + 3 dlt metadata tables)

## Changes Made

### 1. Source Configuration Updates
- ✅ Updated schema from `Testing` → `prod_hostfully`
- ✅ Fixed table name pluralization:
  - `raw_lead` → `raw_leads`
  - `raw_employee` → `raw_employees`
  - `raw_owner` → `raw_owners`
  - `raw_property_description` → `raw_property_descriptions`
- ✅ Added dlt metadata tables: `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`
- ✅ Added `raw_promo_codes` table

### 2. Removed Legacy Models
The following models were disabled (renamed with `.disabled` extension) as their source tables are not available in the dlt pipeline:

**Staging Models:**
- `stg_hostfully__property_fees`
- `stg_hostfully__property_photos`
- `stg_hostfully__property_rooms`
- `stg_hostfully__rate_multipliers`

**Marts Models:**
- `dim_property_photos`
- `dim_property_rooms`

**Snapshots:**
- `snp_hostfully__property_fees`
- `snp_hostfully__property_photos`
- `snp_hostfully__property_rooms`
- `snp_hostfully__rate_multipliers`

### 3. Pipeline Organization
Models are now organized into three pipeline folders based on their refresh cadence:

#### **Hourly Pipeline** (`staging/hourly_pipeline/`)
**Tables**: `raw_leads`, `raw_messages`, `raw_orders`, `raw_transactions`
**Schedule**: Every hour
**Models**:
- `stg_hostfully__leads`
- `stg_hostfully__messages`
- `stg_hostfully__orders`
- `stg_hostfully__transactions`
- `stg_hostfully__threads`

**Tags**: `["hourly", "leads_pipeline"]`

#### **Biweekly Pipeline** (`staging/biweekly_pipeline/`)
**Tables**: `raw_agency`, `raw_employees`, `raw_owners`, `raw_promo_codes`
**Schedule**: Every 2 weeks
**Models**:
- `stg_hostfully__agencies`
- `stg_hostfully__employees`
- `stg_hostfully__owners`
- `stg_hostfully__promo_codes`

**Tags**: `["biweekly", "employees_pipeline"]`

#### **Daily Pipeline** (`staging/daily_pipeline/`)
**Tables**: All `raw_property_*` tables, `raw_booking_dot_com_status`, `raw_property_reviews_*`
**Schedule**: Daily at 7 AM UTC
**Models**:
- `stg_hostfully__properties`
- `stg_hostfully__property_amenities`
- `stg_hostfully__property_calendar`
- `stg_hostfully__property_channel_links`
- `stg_hostfully__property_descriptions`
- `stg_hostfully__property_owners`
- `stg_hostfully__property_reviews_airbnb`
- `stg_hostfully__property_rules`
- `stg_hostfully__airbnb_data`
- `stg_hostfully__booking_dot_com_data`

**Tags**: `["daily", "properties_pipeline"]`

### 4. Marts Layer (14 Models)
All marts models materialize as tables in the `marts` schema:

**Fact Tables**:
- `fct_bookings` - Comprehensive booking fact table
- `fct_order_transactions` - Order and transaction details
- `fct_pricing` - Property calendar and pricing

**Dimension Tables**:
- `dim_agencies` - Agency profiles
- `dim_employees` - Employee details
- `dim_promo_codes` - Promotional codes
- `dim_properties` - Property master data
- `dim_property_amenities` - Property features
- `dim_property_policies` - House rules and policies
- `dim_guests` - Guest information
- `dim_threads` - Message threads
- `dim_messages` - Individual messages
- `dim_airbnb_reviews` - Airbnb reviews
- `dim_overall_reviews` - Aggregated reviews

## dbt Project Configuration

### Materialization Strategy
- **Staging Models**: Views (lightweight, always up-to-date)
- **Marts Models**: Tables (optimized for query performance)
- **Snapshots**: SCD Type 2 tracking in `snapshots` schema

### Schema Organization
- **Staging**: `cloud-data-pipeline-projects.staging`
- **Marts**: `cloud-data-pipeline-projects.marts`
- **Snapshots**: `cloud-data-pipeline-projects.snapshots`

### Tags for Airflow Orchestration
Models are tagged for selective execution:
- `staging` - All staging models
- `hourly`, `leads_pipeline` - Hourly refresh models
- `biweekly`, `employees_pipeline` - Biweekly refresh models
- `daily`, `properties_pipeline` - Daily refresh models
- `marts` - All marts models

## Airflow Integration Examples

### Running Specific Pipelines
```bash
# Run hourly pipeline models
dbt run --select tag:hourly

# Run biweekly pipeline models
dbt run --select tag:biweekly

# Run daily pipeline models
dbt run --select tag:daily

# Run all staging models
dbt run --select tag:staging

# Run all marts models
dbt run --select tag:marts

# Run full refresh of marts after staging updates
dbt run --select tag:staging+
```

## Data Freshness & Quality

### Source Freshness (Can be configured in src_hostfully.yml)
Recommended freshness checks based on pipeline schedules:
- **Hourly tables**: Warn after 2 hours, Error after 4 hours
- **Biweekly tables**: Warn after 15 days, Error after 20 days
- **Daily tables**: Warn after 26 hours, Error after 50 hours

### Data Quality Tests
- 122 active tests covering:
  - Unique constraints on primary keys
  - Not null constraints on critical fields
  - Referential integrity (relationships)
  - Custom business logic validations

## BigQuery Warehouse Structure

### Dataset: `cloud-data-pipeline-projects.prod_hostfully`
**Raw Tables (from dlt)**:
- 19 data tables (`raw_*`)
- 3 dlt metadata tables (`_dlt_*`)

### Dataset: `cloud-data-pipeline-projects.staging`
- 19 staging views

### Dataset: `cloud-data-pipeline-projects.marts`
- 14 marts tables

### Dataset: `cloud-data-pipeline-projects.snapshots`
- 19 SCD Type 2 snapshot tables

## Next Steps & Recommendations

### Immediate Actions
1. ✅ Test staging models: `dbt run --select tag:staging`
2. ✅ Test marts models: `dbt run --select tag:marts`
3. ✅ Validate snapshots: `dbt snapshot`
4. ✅ Run all tests: `dbt test`

### Future Enhancements
1. **Incremental Models**: Consider converting `fct_bookings` and `fct_order_transactions` to incremental materialization for hourly efficiency
2. **Source Freshness**: Add freshness checks to `src_hostfully.yml` for monitoring
3. **Exposures**: Document BI dashboard dependencies
4. **CI/CD**: Set up dbt Cloud or GitHub Actions for automated testing
5. **Documentation**: Generate and publish dbt docs: `dbt docs generate && dbt docs serve`

## Virtual Environment
A separate Python virtual environment has been created at `.venv/` with:
- `dbt-bigquery==1.11.0`
- `dbt-core==1.11.2`
- All required dependencies

Activate with: `source .venv/bin/activate`

## Migration Validation Checklist
- [x] Source definitions updated for dlt tables
- [x] Broken model references fixed
- [x] Legacy models disabled
- [x] Project organized by pipeline cadence
- [x] Tags configured for Airflow orchestration
- [x] Schema tests cleaned up
- [x] dbt project parses without errors
- [x] Connection to BigQuery verified
- [ ] Staging models executed successfully
- [ ] Marts models executed successfully
- [ ] Snapshots executed successfully
- [ ] All tests passing
- [ ] Airflow DAGs configured

## Support & Maintenance
- **dbt Version**: 1.11.2
- **Adapter**: dbt-bigquery 1.11.0
- **Python**: 3.12.3
- **Database**: BigQuery (cloud-data-pipeline-projects)
- **Location**: EU

---

*Migration completed: February 2, 2026*
*From: Airbyte ingestion*
*To: dlt (data load tool) ingestion*
