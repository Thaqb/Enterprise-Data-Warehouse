with source as (
    select * from {{ source('hostfully_api', 'raw_owners') }}
),

renamed as (
    select
        -- ids
        uid as owner_id,

        -- owner details
        {{ initcap('first_name') }} as first_name,
        {{ initcap('last_name') }} as last_name,
        email,
        -- phone details: strip non-digits and handle "null" string literal
        nullif(regexp_replace(phone_number, r'\D', ''), 'null') as phone_number,
        phone_number_area_code as phone_area_code,
        -- country logic: derive from area code
        case 
            when cast(phone_number_area_code as string) in ('1') then 'USA'
            when cast(phone_number_area_code as string) in ('44') then 'United Kingdom'
            when cast(phone_number_area_code as string) in ('33') then 'France'
            when cast(phone_number_area_code as string) in ('49') then 'Germany'
            when cast(phone_number_area_code as string) in ('34') then 'Spain'
            when cast(phone_number_area_code as string) in ('39') then 'Italy'
            when cast(phone_number_area_code as string) in ('20') then 'Egypt'
            when cast(phone_number_area_code as string) in ('971') then 'UAE'
            when cast(phone_number_area_code as string) in ('61') then 'Australia'
            else 'Unknown'
        end as country,
        is_active as is_active,
        
        -- Converting JSON to STRING before aggregating
        (
            select string_agg(json_value(perm), ', ') 
            from unnest(json_query_array(permission_types)) as perm
        ) as permissions_list,

        -- financial & sync details
        cast(agency_commission_rate as numeric) as owner_commission_rate,
        external_calendar_url as external_calendar_url,
        picture_url as picture_url

    from source
)

select * from renamed