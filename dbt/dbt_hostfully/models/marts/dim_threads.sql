with message_ranking as (
    select 
        thread_id,
        sender_type,
        created_at_utc,
        -- Get the timestamp and sender of the previous message
        lag(created_at_utc) over (partition by thread_id order by created_at_utc) as prev_ts,
        lag(sender_type) over (partition by thread_id order by created_at_utc) as prev_sender
    from {{ ref('stg_hostfully__messages') }}
),

employee_response_times as (
    select 
        thread_id,
        -- Calculate difference ONLY when Guest was followed by Agency
        timestamp_diff(created_at_utc, prev_ts, MINUTE) as response_time_minutes
    from message_ranking
    where prev_sender = 'GUEST' 
      and sender_type = 'AGENCY'
),

thread_stats as (
    select 
        thread_id,
        count(message_id) as total_message_count,
        max(created_at_utc) as last_message_at
    from {{ ref('stg_hostfully__messages') }}
    group by thread_id
),

employee_performance as (
    select 
        thread_id,
        avg(response_time_minutes) as avg_staff_response_time_minutes,
        count(response_time_minutes) as total_staff_responses -- How many times they replied
    from employee_response_times
    group by thread_id
),

last_message_details as (
    select 
        thread_id,
        sender_type as last_message_sender_type
    from {{ ref('stg_hostfully__messages') }}
    qualify row_number() over (partition by thread_id order by created_at_utc desc) = 1
)

select  
    t.* except (extracted_email), 
    ts.total_message_count,
    ts.last_message_at,
    lmd.last_message_sender_type,
    round(ep.avg_staff_response_time_minutes, 2) as avg_staff_response_time_minutes,
    coalesce(ep.total_staff_responses, 0) as total_staff_responses
from {{ ref('stg_hostfully__threads') }} t
left join thread_stats ts using (thread_id)
left join employee_performance ep using (thread_id)
left join last_message_details lmd using (thread_id)