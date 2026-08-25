with source as (
    select * from {{ source('hostfully_api', 'raw_promo_codes') }}
),

renamed as (
    select
        -- ids
        uid as promo_code_id,
        coalesce(property_uid, 'All') as property_id,
        agency_uid as agency_id,

        -- promo details
        code as promo_code,
        type as discount_type, -- e.g., PERCENTAGE or AMOUNT
        cast(value as numeric) as discount_value,
        status as promo_status,
        single_usage as is_single_usage,

        -- validity dates (casting to timestamp)
        safe_cast(valid_from_utc_date_time as timestamp) as valid_from_utc,
        safe_cast(valid_to_utc_date_time as timestamp) as valid_to_utc,
        safe_cast(expiration_utc_date_time as timestamp) as expired_at_utc

    from source
)

select * from renamed