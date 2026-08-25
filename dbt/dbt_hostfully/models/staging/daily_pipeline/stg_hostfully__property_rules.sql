with source as (
    select * from {{ source('hostfully_api', 'raw_property_rules') }}
),

renamed as (
    select
        -- generated primary key (combination of property and rule)
        {{ dbt_utils.generate_surrogate_key(['_properties_uid', 'rule']) }} as property_rule_id,

        -- ids
        _properties_uid as property_id,

        -- rule details
        rule as rule_name,
        
        -- channel applicability (flattened from JSON)
        coalesce(cast(json_value(channels, '$.AIRBNB') as bool)) as is_airbnb_applicable,
        coalesce(cast(json_value(channels, '$.BOOKINGDOTCOM') as bool)) as is_booking_dot_com_applicable,
        coalesce(cast(json_value(channels, '$.GOOGLE') as bool)) as is_google_applicable,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed