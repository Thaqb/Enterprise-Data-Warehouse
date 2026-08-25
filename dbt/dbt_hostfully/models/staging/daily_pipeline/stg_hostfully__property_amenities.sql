with source as (
    select * from {{ source('hostfully_api', 'raw_property_amenities') }}
),

renamed as (
    select
        -- ids
        uid as property_amenity_id,
        property_uid as property_id,

        -- amenity details
        amenity,
        category,
        description,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed