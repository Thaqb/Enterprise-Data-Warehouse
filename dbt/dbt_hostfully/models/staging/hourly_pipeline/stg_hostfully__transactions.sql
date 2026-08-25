with raw_transactions as (
    select * from {{ source('hostfully_api', 'raw_transactions') }}
),

renamed as (
    select
        -- ids
        uid as transaction_id,
        order_uid as order_id,

        -- transaction details
        type as transaction_type,   -- e.g., PAYMENT, REFUND
        status as transaction_status, -- e.g., SUCCESS, FAILED
        cast(amount as numeric) as transaction_amount,
        manual as is_manual_transaction,

        -- dates
        safe_cast(created_zoned_date_time as timestamp) as created_at_utc

    from raw_transactions
)

select * from renamed