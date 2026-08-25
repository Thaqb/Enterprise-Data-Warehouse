with guest_details as (
    select * from {{ ref('fct_bookings') }}
    where guest_id is not null
    and lead_current_status = 'BOOKED'
),

-- Bring in your successful transaction data
transactions as (
    select 
        order_id,
        sum(case 
            when transaction_type = 'SALE' then transaction_amount 
            when transaction_type = 'REFUND' then -transaction_amount 
            else 0 
        end) as net_order_revenue
    from {{ ref('stg_hostfully__transactions') }}
    where transaction_status = 'SUCCESS'
    group by 1
),

country_codes as(
    select * from {{ ref('stg_hostfully__country_codes') }}
),

phone_country_matches as (
    select
        l.guest_id,
        l.guest_phone_number,
        cast(cc.calling_code as string) as calling_code,
        cc.country_name,
        cc.expected_regex,
        length(cast(cc.calling_code as string)) as code_length,
        case 
            when l.guest_phone_number like '+' || cast(cc.calling_code as string) || '%' then 1
            else 0
        end as is_match
    from guest_details l
    cross join country_codes cc
    where l.guest_phone_number is not null
),

best_match as (
    select
        guest_id,
        guest_phone_number,
        calling_code as phone_country_code,
        country_name as inferred_country,
        expected_regex,
        row_number() over (
            partition by guest_id, guest_phone_number 
            order by code_length desc
        ) as match_rank
    from phone_country_matches
    where is_match = 1
),

guest_with_parsed_phone as (
    select
        l.*,
        bm.phone_country_code,
        bm.inferred_country,
        case
            when bm.phone_country_code is not null then
                regexp_replace(l.guest_phone_number, r'^\+' || bm.phone_country_code, '')
            else null
        end as local_phone_number,
        bm.expected_regex,
        case
            when l.guest_phone_number is null then null
            when bm.phone_country_code is null then false 
            when bm.expected_regex is null then null 
            when regexp_contains(
                regexp_replace(l.guest_phone_number, r'^\+' || bm.phone_country_code, ''),
                bm.expected_regex
            ) then true
            else false
        end as is_valid,
        -- Join transaction revenue to the lead
        coalesce(t.net_order_revenue, 0) as lead_revenue
    from guest_details l
    left join best_match bm
        on l.guest_id = bm.guest_id
        and l.guest_phone_number = bm.guest_phone_number
        and bm.match_rank = 1
    left join transactions t 
        on l.order_id = t.order_id
),

final as (
    select
        l.guest_id, 
        l.guest_first_name,
        l.guest_last_name,
        l.guest_email,
        l.guest_phone_number,
        l.phone_country_code,
        l.local_phone_number,
        l.inferred_country,
        l.is_valid,
        
        -- Behavioral Metrics from Leads
        min(booked_at_utc) as first_booked_at,
        max(booked_at_utc) as last_booked_at,
        count(lead_id) as total_bookings,
        
        -- The new revenue column
        sum(lead_revenue) as total_paid_per_stays,
        
        -- Metadata
        min(l.lead_created_at_utc) as profile_created_at
        
    from guest_with_parsed_phone l
    group by 1,2,3,4,5,6,7,8,9
)

select * from final