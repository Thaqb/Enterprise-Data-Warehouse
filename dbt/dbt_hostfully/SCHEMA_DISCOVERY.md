# Schema Discovery Queries

Please run these queries in BigQuery to get the column schemas:

## 1. Check raw_employees columns:
```sql
SELECT column_name, data_type
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_employees'
ORDER BY ordinal_position;
```

## 2. Check raw_agency columns:
```sql
SELECT column_name, data_type
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_agency'
ORDER BY ordinal_position;
```

## 3. Check raw_properties columns:
```sql
SELECT column_name, data_type  
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_properties'
ORDER BY ordinal_position;
```

## 4. Check raw_leads columns:
```sql
SELECT column_name, data_type
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_leads'
ORDER BY ordinal_position;
```

## 5. Check raw_orders columns:
```sql
SELECT column_name, data_type
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_orders'
ORDER BY ordinal_position;
```
