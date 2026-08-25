with source as (
    select * from {{ source('hostfully_api', 'raw_property_owner') }}
),

renamed as (
    select
        -- generated primary key for the link
        {{ dbt_utils.generate_surrogate_key(['json_value(owner, "$.uid")', 'property_uid']) }} as property_owner_link_id,

        -- foreign keys
        json_value(owner, '$.uid') as owner_id,
        property_uid as property_id,

        -- link-specific settings
        copy_owner_when_booking_confirmed as copy_owner_when_booking_confirmed,
        collect_and_remit_taxes as collect_and_remit_taxes,
        owner_notes as owner_notes,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed