with raw_messages as (
    select * from {{ source('hostfully_api', 'raw_messages') }}
),

-- Deduplicate exact duplicate rows (keep first occurrence by extraction time)
deduplicated as (
    select * except(rn)
    from (
        select
            *,
            row_number() over (
                partition by uid, thread_uid, lead_uid, created_utc_date_time, sender_type, json_extract_scalar(content, '$.text')
                order by _dlt_load_id asc
            ) as rn
        from raw_messages
    )
    where rn = 1
),

renamed as (
    select
        -- Surrogate key (handles exact duplicates)
        {{ dbt_utils.generate_surrogate_key(['uid', 'thread_uid', 'lead_uid', 'created_utc_date_time']) }} as message_sk,
        
        -- Business keys
        uid as message_id,
        thread_uid as thread_id,
        lead_uid as lead_id,

        -- message details
        type as message_channel, -- e.g., AIRBNB
        status as message_status,  -- e.g., SENT, CREATED ,FAILED
        sender_type as sender_type, -- e.g., AGENCY, GUEST
        
        -- content extraction
        json_value(content, '$.subject') as message_subject,
        json_value(content, '$.text') as message_text,

        json_value(attachments[0], '$.uri') as attachment_url,

        -- metadata
        safe_cast(created_utc_date_time as timestamp) as created_at_utc

    from deduplicated
)

select * from renamed