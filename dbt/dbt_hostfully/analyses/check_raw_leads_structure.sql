-- Query to check raw_leads table structure and sample metadata column
SELECT 
    uid,
    metadata,
    _dlt_load_id,
    _dlt_id
FROM {{ source('hostfully_api', 'raw_leads') }}
LIMIT 1
