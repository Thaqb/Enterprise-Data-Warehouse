with amenities as (
    select * from {{ ref('stg_hostfully__property_amenities') }}
),
properties as (
    select distinct property_id from {{ ref('stg_hostfully__properties') }}
),
pivoted as (
    select
        p.property_id,
        -- Using coalesce to ensure we get 'false' instead of 'null' for properties with no amenities
        coalesce(max(case when amenity in ('HAS_INTERNET_WIFI', 'HAS_PAID_WIFI') then true end), false) as has_wifi,
        coalesce(max(case when amenity in ('HAS_POOL', 'HAS_HEATED_POOL', 'HAS_INDOOR_POOL', 'HAS_COMMUNAL_POOL') then true end), false) as has_pool,
        coalesce(max(case when amenity = 'HAS_KITCHEN' then true end), false) as has_full_kitchen,
        coalesce(max(case when amenity = 'HAS_AIR_CONDITIONING' then true end), false) as has_ac,
        coalesce(max(case when amenity in ('HAS_FREE_PARKING', 'HAS_FREE_STREET_PARKING') then true end), false) as has_free_parking,
        coalesce(max(case when amenity in ('HAS_JACUZZI', 'HAS_HOT_TUB') then true end), false) as has_hot_tub,
        coalesce(max(case when amenity = 'HAS_WASHER' then true end), false) as has_washer,
        coalesce(max(case when amenity in ('HAS_SMOKE_DETECTOR', 'HAS_FIRE_EXTINGUISHER') then true end), false) as has_safety_basics,
        
    from properties p
    left join amenities a on p.property_id = a.property_id
    group by 1
)

select * from pivoted