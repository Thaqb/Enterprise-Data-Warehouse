-- Get all columns for property_descriptions
SELECT column_name
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'raw_property_descriptions'
ORDER BY ordinal_position
