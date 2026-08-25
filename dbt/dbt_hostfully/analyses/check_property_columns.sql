--  Query columns for failing property models
SELECT table_name, column_name, data_type
FROM `cloud-data-pipeline-projects.prod_hostfully.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN (
    'raw_property_descriptions',
    'raw_property_rules', 
    'raw_property_owners',
    'raw_airbnb_data',
    'raw_property_calendar',
    'raw_property_channel_links',
    'raw_booking_dot_com_data',
    'raw_property_reviews_airbnb',
    'raw_property_reviews_booking',
    'raw_property_amenities',
    'raw_properties',
    'raw_owners',
    'raw_promo_codes'
)
ORDER BY table_name, ordinal_position
