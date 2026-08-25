

with calendar_source as (
    select * from {{ ref('stg_hostfully__property_calendar') }}
),

final as (
    select
        property_calendar_id,
        property_id,
        calendar_date,
        
        -- Time Intelligence
        extract(year from calendar_date) as year,
        extract(month from calendar_date) as month,
        format_date('%B', calendar_date) as month_name,
        extract(dayofweek from calendar_date) as day_of_week,
        case 
            when extract(dayofweek from calendar_date) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- Pricing
        price_amount,
        currency,

        -- Availability Logic
        is_unavailable,
        not is_unavailable as is_available,
        unavailability_reason,
        
        -- Derived Status Flags based on image data
        case 
            when unavailability_reason = 'BOOKING' then true 
            else false 
        end as is_booked, -- Actual reservation

        case 
            when unavailability_reason = 'BLOCK' then true 
            else false 
        end as is_admin_blocked, -- Manual block by owner/manager

        case 
            when unavailability_reason = 'INQUIRY' then true 
            else false 
        end as has_active_inquiry, -- Potential booking in progress

        -- Stay Constraints
        min_stay_length,
        max_stay_length,
        
        -- Check-in/out Flags
        is_available_for_check_in,
        is_available_for_check_out
        
    from calendar_source
)

select * from final