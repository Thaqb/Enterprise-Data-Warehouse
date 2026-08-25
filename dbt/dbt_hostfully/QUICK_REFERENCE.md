# dbt Hostfully - Quick Reference Guide

## Environment Setup
```bash
# Navigate to dbt project
cd /home/mahmoud/DWH/dbt/dbt_hostfully

# Activate virtual environment
source .venv/bin/activate

# Verify connection
dbt debug
```

## Running Models by Pipeline

### Hourly Pipeline (Leads, Orders, Messages, Transactions)
```bash
# Run all hourly staging models
dbt run --select tag:hourly

# Run specific hourly model
dbt run --select stg_hostfully__leads

# Test hourly models
dbt test --select tag:hourly
```

### Biweekly Pipeline (Agency, Employees, Owners, Promo Codes)
```bash
# Run all biweekly staging models
dbt run --select tag:biweekly

# Test biweekly models
dbt test --select tag:biweekly
```

### Daily Pipeline (Properties and Related)
```bash
# Run all daily staging models
dbt run --select tag:daily

# Test daily models
dbt test --select tag:daily
```

### All Staging Models
```bash
# Run all staging models
dbt run --select tag:staging

# Test all staging models
dbt test --select tag:staging
```

### Marts Layer
```bash
# Run all marts models
dbt run --select tag:marts

# Run specific mart
dbt run --select fct_bookings

# Test marts
dbt test --select tag:marts
```

## Snapshots (SCD Type 2)
```bash
# Run all snapshots
dbt snapshot

# Run specific snapshot
dbt snapshot --select snp_hostfully__leads
```

## Full Workflows

### Complete Hourly Refresh
```bash
# 1. Run hourly staging models
dbt run --select tag:hourly

# 2. Run downstream marts that depend on hourly data
dbt run --select tag:hourly+

# 3. Run tests
dbt test --select tag:hourly+

# 4. Update snapshots for hourly tables
dbt snapshot --select snp_hostfully__leads snp_hostfully__messages snp_hostfully__orders snp_hostfully__transactions
```

### Complete Daily Refresh
```bash
# 1. Run daily staging models
dbt run --select tag:daily

# 2. Run downstream marts
dbt run --select tag:daily+

# 3. Run tests
dbt test --select tag:daily+

# 4. Update snapshots
dbt snapshot --select tag:daily
```

### Complete Biweekly Refresh
```bash
# 1. Run biweekly staging models
dbt run --select tag:biweekly

# 2. Run downstream marts
dbt run --select tag:biweekly+

# 3. Run tests
dbt test --select tag:biweekly+

# 4. Update snapshots
dbt snapshot --select tag:biweekly
```

### Full Project Run
```bash
# 1. Run all staging
dbt run --select tag:staging

# 2. Run all marts
dbt run --select tag:marts

# 3. Run all snapshots
dbt snapshot

# 4. Run all tests
dbt test
```

## Selective Runs

### Run specific model and all downstream
```bash
dbt run --select stg_hostfully__properties+
```

### Run specific model and all upstream
```bash
dbt run --select +fct_bookings
```

### Run specific model and all related (up and down)
```bash
dbt run --select +fct_bookings+
```

### Run modified models only
```bash
dbt run --select state:modified+
```

## Testing Commands

### Run all tests
```bash
dbt test
```

### Run tests for specific model
```bash
dbt test --select stg_hostfully__leads
```

### Run specific test types
```bash
# Unique tests only
dbt test --select test_type:unique

# Relationship tests only
dbt test --select test_type:relationships

# Not null tests only
dbt test --select test_type:not_null
```

## Source Freshness Checks
```bash
# Check freshness of all sources
dbt source freshness

# Check specific source
dbt source freshness --select source:hostfully_api
```

## Documentation

### Generate and serve documentation
```bash
# Generate docs
dbt docs generate

# Serve docs locally
dbt docs serve
```

## Debugging

### Compile SQL without running
```bash
dbt compile --select stg_hostfully__leads
```

### Show compiled SQL
```bash
dbt show --select stg_hostfully__leads
```

### List all models
```bash
# All models
dbt list --resource-type model

# Staging models only
dbt list --select tag:staging

# Marts models only
dbt list --select tag:marts
```

### Parse project
```bash
dbt parse
```

## Airflow DAG Examples

### Hourly DAG (runs every hour)
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 2),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_hostfully_hourly',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    
    dbt_run_hourly = BashOperator(
        task_id='dbt_run_hourly_staging',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:hourly',
    )
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_hourly_marts',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:hourly+',
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_hourly',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt test --select tag:hourly+',
    )
    
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_hourly',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt snapshot --select snp_hostfully__leads snp_hostfully__messages snp_hostfully__orders snp_hostfully__transactions',
    )
    
    dbt_run_hourly >> dbt_run_marts >> dbt_test >> dbt_snapshot
```

### Daily DAG (runs at 7 AM UTC)
```python
with DAG(
    'dbt_hostfully_daily',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # 7 AM UTC
    catchup=False,
) as dag:
    
    dbt_run_daily = BashOperator(
        task_id='dbt_run_daily_staging',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:daily',
    )
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_daily_marts',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:daily+',
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_daily',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt test --select tag:daily+',
    )
    
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_daily',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt snapshot',
    )
    
    dbt_run_daily >> dbt_run_marts >> dbt_test >> dbt_snapshot
```

### Biweekly DAG (runs every 2 weeks)
```python
with DAG(
    'dbt_hostfully_biweekly',
    default_args=default_args,
    schedule_interval='0 0 */14 * *',  # Every 14 days at midnight
    catchup=False,
) as dag:
    
    dbt_run_biweekly = BashOperator(
        task_id='dbt_run_biweekly_staging',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:biweekly',
    )
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_biweekly_marts',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt run --select tag:biweekly+',
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test_biweekly',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt test --select tag:biweekly+',
    )
    
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot_biweekly',
        bash_command='cd /home/mahmoud/DWH/dbt/dbt_hostfully && source .venv/bin/activate && dbt snapshot',
    )
    
    dbt_run_biweekly >> dbt_run_marts >> dbt_test >> dbt_snapshot
```

## Model Organization

```
models/
├── staging/
│   ├── hourly_pipeline/        # 5 models (hourly refresh)
│   │   ├── stg_hostfully__leads.sql
│   │   ├── stg_hostfully__messages.sql
│   │   ├── stg_hostfully__orders.sql
│   │   ├── stg_hostfully__threads.sql
│   │   └── stg_hostfully__transactions.sql
│   ├── biweekly_pipeline/      # 4 models (biweekly refresh)
│   │   ├── stg_hostfully__agencies.sql
│   │   ├── stg_hostfully__employees.sql
│   │   ├── stg_hostfully__owners.sql
│   │   └── stg_hostfully__promo_codes.sql
│   └── daily_pipeline/         # 10 models (daily refresh)
│       ├── stg_hostfully__properties.sql
│       ├── stg_hostfully__property_*.sql (8 models)
│       ├── stg_hostfully__airbnb_data.sql
│       └── stg_hostfully__booking_dot_com_data.sql
└── marts/                      # 14 models
    ├── fct_bookings.sql
    ├── fct_order_transactions.sql
    ├── fct_pricing.sql
    └── dim_*.sql (11 dimension models)
```

## Troubleshooting

### Connection issues
```bash
dbt debug
```

### Check if model compiles
```bash
dbt compile --select model_name
```

### View model lineage
```bash
dbt docs generate
dbt docs serve
# Navigate to model and click "View Lineage Graph"
```

### Clear cache
```bash
dbt clean
```

### Force full refresh of incremental models
```bash
dbt run --select model_name --full-refresh
```

---

**Project Location**: `/home/mahmoud/DWH/dbt/dbt_hostfully`
**Virtual Environment**: `.venv/`
**dbt Version**: 1.11.2
**Adapter**: dbt-bigquery 1.11.0
