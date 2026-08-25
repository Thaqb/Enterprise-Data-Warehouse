with raw_messages as (
    select * from {{ source('hostfully_api', 'raw_messages') }}
),

-- Step 1: Extract body and cast timestamp ONCE (Performance)
parsed_messages as (
    select 
        thread_uid as thread_id,
        lead_uid as lead_id,
        uid as message_uid,
        json_extract_scalar(content, '$.text') as message_body,
        safe_cast(created_utc_date_time as timestamp) as created_at_utc
    from raw_messages
    where thread_uid is not null
),

-- Step 2: Extract email from ANY message in the thread
latest_emails as (
    select 
        thread_id,
        email
    from (
        select 
            thread_id,
            regexp_extract(message_body, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') as email,
            row_number() over (
                partition by thread_id 
                order by created_at_utc desc
            ) as email_rank
        from parsed_messages
        where message_body is not null
          and regexp_contains(message_body, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
    )
    where email_rank = 1
),

-- Step 3: Aggregate (Now we filter for lead_id only for the counts)
thread_aggregation as (
    select
        thread_id,
        lead_id,
        min(created_at_utc) as created_at_utc,
        max(created_at_utc) as last_updated_at_utc,
        count(distinct message_uid) as message_count
    from parsed_messages
    where lead_id is not null
    group by thread_id, lead_id
),

-- Final Output with deduplication
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['t.thread_id', 't.lead_id']) }} as thread_sk,
        t.thread_id,
        t.lead_id,
        e.email as extracted_email,
        t.created_at_utc,
        t.last_updated_at_utc,
        t.message_count,
        row_number() over (partition by {{ dbt_utils.generate_surrogate_key(['t.thread_id', 't.lead_id']) }} order by t.last_updated_at_utc desc) as row_num
    from thread_aggregation t
    left join latest_emails e on t.thread_id = e.thread_id
)

select 
    thread_sk,
    thread_id,
    lead_id,
    extracted_email,
    created_at_utc,
    last_updated_at_utc,
    message_count
from renamed
where row_num = 1