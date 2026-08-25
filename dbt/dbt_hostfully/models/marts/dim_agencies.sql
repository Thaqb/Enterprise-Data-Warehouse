WITH agencies AS
(
	SELECT  *
	FROM {{ ref('stg_hostfully__agencies') }}
),
properties AS
(
    SELECT  count(property_id) as total_properties,
            agency_id
    FROM {{ ref('stg_hostfully__properties') }}
    where is_active = true
    group by agency_id
    
)
SELECT
    a.*,
    p.total_properties as total_active_properties
FROM agencies a
join properties p
    on a.agency_id = p.agency_id