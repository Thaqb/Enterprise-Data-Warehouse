# Airflow Deployment Guide - Hostfully Data Pipelines

## Overview
This deployment sets up 3 Airflow DAGs orchestrating dlt extraction and dbt transformation:
- **hostfully_leads_pipeline**: Hourly - Leads, Orders, Transactions, Messages
- **hostfully_employees_pipeline**: Biweekly - Agencies, Employees, Owners, Promo Codes
- **hostfully_properties_pipeline**: Daily - Properties, Calendar, Reviews + Snapshots

## Files Created

| File | Description |
|------|-------------|
| `Dockerfile` | Custom Airflow image with Cosmos + dbt-bigquery |
| `docker-compose.yaml` | Updated with build config, volumes, SMTP env vars |
| `.env` | Fernet key, SMTP credentials, paths |
| `dags/hostfully_leads_dag.py` | Hourly pipeline DAG |
| `dags/hostfully_employees_dag.py` | Biweekly pipeline DAG |
| `dags/hostfully_properties_dag.py` | Daily pipeline DAG + snapshots |

## Pre-Deployment Checklist

1. **Verify dlt venv exists**: `/home/mahmoud/ingestion_layer/.venv/bin/activate`
2. **Verify dbt venv exists**: `/home/mahmoud/DWH/dbt/dbt_hostfully/.venv/bin/activate`
3. **Verify GCP credentials**: `/home/mahmoud/DWH/cloud-data-pipeline-projects-dd84571727d2.json`
4. **Verify Gmail App Password**: `itvg hojr lhcx kkvu` (stored in .env)

## Deployment Steps

### Step 1: Build Custom Airflow Image

```bash
cd /home/mahmoud/airflow-prod-compose
docker compose build
```

This builds the custom image with astronomer-cosmos, dbt-bigquery, and Google provider.

### Step 2: Start Airflow Services

```bash
docker compose up -d
```

Wait ~60 seconds for all services to initialize.

### Step 3: Create Admin User (First Time Only)

```bash
docker compose exec airflow-apiserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email mahmoudmostafa@partment.co \
    --password admin
```

### Step 4: Access Airflow UI

Open browser: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin` (or whatever you set)

### Step 5: Create BigQuery Connection

In Airflow UI:
1. Go to **Admin** → **Connections**
2. Click **+** (Add a new record)
3. Fill in:
   - **Connection ID**: `bigquery_conn`
   - **Connection Type**: `Google Cloud`
   - **Keyfile Path**: `/opt/airflow/secrets/gcp_credentials.json`
   - **Project ID**: `cloud-data-pipeline-projects`
   - **Scopes**: Leave default or add `https://www.googleapis.com/auth/bigquery`
4. Click **Save**

### Step 6: Verify DAG Parsing

Check that all 3 DAGs appear without errors:
- `hostfully_leads_pipeline`
- `hostfully_employees_pipeline`
- `hostfully_properties_pipeline`

If you see parsing errors, check logs:
```bash
docker compose logs airflow-dag-processor
```

### Step 7: Test Run (Optional)

Trigger a manual run of the employees pipeline (safest for testing):
1. In Airflow UI, find `hostfully_employees_pipeline`
2. Click the play button (▶️) to trigger
3. Monitor the Graph view to see tasks execute

## Architecture Details

### Volume Mounts (Host → Container)
- `/home/mahmoud/ingestion_layer` → `/opt/airflow/ingestion_layer`
- `/home/mahmoud/DWH/dbt/dbt_hostfully` → `/opt/airflow/dbt_project`
- `/home/mahmoud/DWH/cloud-data-pipeline-projects-dd84571727d2.json` → `/opt/airflow/secrets/gcp_credentials.json`

### DAG Task Flow

**Leads Pipeline (Hourly)**:
```
dlt_extract_leads → dbt_transform_hourly (TaskGroup)
                    ├── stg_hostfully__leads
                    ├── stg_hostfully__messages
                    ├── stg_hostfully__orders
                    ├── stg_hostfully__transactions
                    ├── stg_hostfully__threads
                    └── [dependent marts: fct_bookings, dim_threads, etc.]
```

**Employees Pipeline (Biweekly - 1st & 15th)**:
```
dlt_extract_employees → dbt_transform_biweekly (TaskGroup)
                        ├── stg_hostfully__agencies
                        ├── stg_hostfully__employees
                        ├── stg_hostfully__owners
                        ├── stg_hostfully__promo_codes
                        └── [dependent marts: dim_agencies, dim_employees, etc.]
```

**Properties Pipeline (Daily 7 AM)**:
```
dlt_extract_properties → dbt_transform_daily (TaskGroup) → dbt_run_snapshots
                         ├── stg_hostfully__properties          ├── snp_fct_bookings
                         ├── stg_hostfully__property_calendar   ├── snp_fct_pricing
                         ├── stg_hostfully__property_reviews_*  ├── snp_dim_properties
                         └── [dependent marts]                  └── snp_hostfully__promo_codes
```

### Email Notifications

Failed tasks will send email to: `mahmoudmostafa@partment.co`

Test SMTP config:
```bash
docker compose exec airflow-apiserver python -c "
from airflow.utils.email import send_email
send_email(
    to='mahmoudmostafa@partment.co',
    subject='Airflow Test Email',
    html_content='<p>SMTP is working!</p>'
)
"
```

## Troubleshooting

### DAG not appearing
- Check `docker compose logs airflow-dag-processor`
- Verify Python syntax: `python -m py_compile dags/<dag_file>.py`

### BigQuery connection error
- Verify `/opt/airflow/secrets/gcp_credentials.json` exists inside container:
  ```bash
  docker compose exec airflow-apiserver ls -la /opt/airflow/secrets/
  ```

### dlt venv not found
- Check mount: `docker compose exec airflow-apiserver ls -la /opt/airflow/ingestion_layer/.venv/`
- Verify host path exists: `ls -la "/home/mahmoud/ingestion_layer/.venv/"`

### dbt venv not found
- Check mount: `docker compose exec airflow-apiserver ls -la /opt/airflow/dbt_project/.venv/`
- Verify host path exists: `ls -la /home/mahmoud/DWH/dbt/dbt_hostfully/.venv/`

### Email not sending
- Verify Gmail App Password is correct in `.env`
- Check Gmail security settings allow "Less secure app access" (not needed with App Password)
- Check logs: `docker compose logs airflow-scheduler | grep -i smtp`

## Monitoring & Maintenance

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-scheduler

# DAG processor (for DAG parsing errors)
docker compose logs -f airflow-dag-processor
```

### Restart Services
```bash
docker compose restart
```

### Stop Services
```bash
docker compose down
```

### Rebuild After Changes
```bash
docker compose down
docker compose build
docker compose up -d
```

## Production Considerations

1. **Change default admin password** after first login
2. **Enable Airflow authentication** (already using FAB auth manager)
3. **Monitor disk space** for logs volume: `/home/mahmoud/airflow-prod-compose/logs/`
4. **Set up log rotation** for dlt/dbt logs
5. **Consider secrets backend** (e.g., Google Secret Manager) instead of .env for production
6. **Add Airflow alerting** for DAG failures beyond email (e.g., Slack, PagerDuty)
7. **Schedule backups** of Airflow metadata database (PostgreSQL volume)

## Next Steps

1. **Unpause DAGs** in Airflow UI to enable automatic scheduling
2. **Monitor first runs** to ensure all pipelines complete successfully
3. **Adjust schedules** if needed (edit DAG files, rebuild is NOT needed)
4. **Add data quality checks** in dbt tests as data evolves
5. **Optimize dbt models** based on Cosmos task execution times in Airflow UI
