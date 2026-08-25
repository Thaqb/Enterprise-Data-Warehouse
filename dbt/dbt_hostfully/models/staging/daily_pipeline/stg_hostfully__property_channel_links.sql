with source as (
    select * from {{ source('hostfully_api', 'raw_property_channel_links') }}
),

renamed as (
    select
        -- generated primary key
        {{ dbt_utils.generate_surrogate_key(['property_uid', 'channel']) }} as channel_link_id,

        -- ids
        property_uid as property_id,

        -- channel details
        channel as channel_name,
        nullif(url, '') as channel_property_url,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed