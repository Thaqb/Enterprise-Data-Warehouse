with source as (
    select * from {{ source('hostfully_api', 'raw_property_descriptions') }}
),

renamed as (
    select
        -- generated primary key
        {{ dbt_utils.generate_surrogate_key(['_properties_uid', 'locale']) }} as property_description_id,

        -- ids
        _properties_uid as property_id,

        -- description details
        locale as desc_language,
        name as public_property_name,
        summary,
        nullif(short_summary, '') as short_summary,
        neighbourhood,
        access,
        space,
        notes,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed