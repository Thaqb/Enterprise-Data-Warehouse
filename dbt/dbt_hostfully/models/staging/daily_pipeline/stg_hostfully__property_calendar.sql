with source as (
    select * from {{ source('hostfully_api', 'raw_property_calendar') }}
),
renamed as (
    select
        -- generated primary key
        {{ dbt_utils.generate_surrogate_key(['property_uid', 'date']) }} as property_calendar_id,
        
        -- ids
        property_uid as property_id,
        
        -- dates
        cast(date as date) as calendar_date,

        -- pricing (using flattened columns)
        cast(pricing__value as numeric) as price_amount,
        pricing__currency as currency,

        -- availability (using flattened columns)
        cast(availability__unavailable as bool) as is_unavailable,
        availability__unavailability_reason as unavailability_reason,
        cast(availability__minimum_stay_length as int64) as min_stay_length,
        cast(availability__maximum_stay_length as int64) as max_stay_length,
        cast(availability__available_for_check_in as bool) as is_available_for_check_in,
        cast(availability__available_for_check_out as bool) as is_available_for_check_out,

        -- other flags
        adjusted_by_custom_pricing_period as is_adjusted_by_custom_pricing

    from source
)

select * from renamed