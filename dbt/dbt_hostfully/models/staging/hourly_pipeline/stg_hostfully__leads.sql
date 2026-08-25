with raw_leads as (
    select * from {{ source('hostfully_api', 'raw_leads') }}
),
threads as (
    select
        t.lead_id as lead_id,
        lower(trim(extracted_email)) as extracted_email
    from {{ ref('stg_hostfully__threads') }} t
),
unnested_guest_information as (
    select
        rl.uid as lead_id,
        -- guest information
        lower(trim(json_value(rl.guest_information, '$.firstName'))) as guest_first_name,
        lower(trim(json_value(rl.guest_information, '$.lastName'))) as guest_last_name,
        -- Email
        coalesce(lower(trim(json_value(rl.guest_information, '$.email'))), t.extracted_email) as guest_email,

        -- Phone Number 
        trim(json_value(rl.guest_information, '$.phoneNumber'))as guest_phone_number,
        

        trim(json_value(rl.guest_information, '$.countryCode')) as guest_country_code,
        cast(json_value(rl.guest_information, '$.adultCount') as int64) as guest_adult_count,
        cast(json_value(rl.guest_information, '$.childrenCount') as int64) as guest_children_count,
        cast(json_value(rl.guest_information, '$.infantCount') as int64) as guest_infant_count,
        cast(json_value(rl.guest_information, '$.petCount') as int64) as guest_pet_count,

    from raw_leads rl
    left join threads t
        on rl.uid = t.lead_id
),
cleaned_guest_information as (
    select
        lead_id,
        {{ clean_name('guest_first_name') }} as guest_first_name,
        {{ clean_name('guest_last_name') }} as guest_last_name,

        {{clean_email('guest_email')}} as guest_email,

        {{clean_phone_number('guest_phone_number')}} as guest_phone_number

    from unnested_guest_information
),
renamed as (
    select
        -- ids
        raw.uid as lead_id,
        raw.property_uid as property_id,
        case 
            when cg.guest_email is null 
            and cg.guest_phone_number is null
            then null
            when cg.guest_email is null 
            and (cg.guest_first_name is null and cg.guest_last_name is null) then null
            else
                {{ dbt_utils.generate_surrogate_key(['cg.guest_email', 'cg.guest_phone_number', 'cg.guest_first_name', 'cg.guest_last_name']) }}
        end as guest_id,

        raw.agency_uid as agency_id,
        raw.external_booking_id as external_booking_id,

        -- lead details
        raw.type as lead_type,
        case 
            when raw.source = 'HOSTFULLY_API' then 'DIRECT_WEBSITE'
            else raw.source
        end as lead_source,
        raw.status as lead_current_status,
        -- guest information
        cg.guest_first_name,
        cg.guest_last_name,
        cg.guest_email,
        cg.guest_phone_number,
        ugi.guest_adult_count,
        ugi.guest_children_count,
        ugi.guest_infant_count,
        ugi.guest_pet_count,
        -- assignee details
        json_value(raw.assignee, '$.uid') as assignee_id,
        json_value(raw.assignee, '$.type') as assignee_type,
        raw.extra_notes as lead_extra_notes,
        

        -- stay dates
        extract(date from safe_cast(raw.check_in_local_date_time as timestamp)) as check_in_date,
        extract(date from safe_cast(raw.check_out_local_date_time as timestamp)) as check_out_date,

        -- metadata dates
        safe_cast(raw.booked_utc_date_time as timestamp) as booked_at_utc,
        safe_cast(json_value(raw.metadata, '$.createdUtcDateTime') as timestamp) as lead_created_at_utc,
        safe_cast(json_value(raw.metadata, '$.updatedUtcDateTime') as timestamp) as lead_updated_at_utc

    from raw_leads raw
    left join unnested_guest_information ugi
        on raw.uid = ugi.lead_id
    left join cleaned_guest_information cg
        on raw.uid = cg.lead_id
)

select * from renamed