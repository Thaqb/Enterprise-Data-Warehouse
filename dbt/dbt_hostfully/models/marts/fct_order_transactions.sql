

with transactions as (
    select * from {{ ref('stg_hostfully__transactions') }}
    where transaction_status = 'SUCCESS' -- Only include successful financial events
),

order_summaries as (
    select
        order_id,
        -- Total amount paid by the guest
        sum(case when transaction_type = 'SALE' then transaction_amount else 0 end) as total_paid_amount,
        
        -- Total amount refunded
        sum(case when transaction_type = 'REFUND' then transaction_amount else 0 end) as total_refunded_amount,
        
        -- Net Revenue (Payments - Refunds)
        sum(
            case 
                when transaction_type = 'SALE' then transaction_amount 
                when transaction_type = 'REFUND' then -transaction_amount 
                else 0 
            end
        ) as net_transaction_amount,

        -- Transaction Timeline
        min(created_at_utc) as first_payment_at,
        max(created_at_utc) as last_transaction_at,
        
        -- Count of transactions (useful for identifying split payments)
        count(transaction_id) as transaction_count,
        
        -- Flag for manual intervention
        logical_or(is_manual_transaction) as has_manual_transaction

    from transactions
    group by order_id
)

select * from order_summaries