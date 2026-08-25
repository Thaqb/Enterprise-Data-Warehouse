with source as (
    select * from {{ source('hostfully_api', 'raw_agency') }}
),

renamed as (
    select
        -- ids
        uid as agency_id,

        -- agency details
        name as agency_name,
        agency_email_address as email,
        phone_number,
        website,
        currency as base_currency,

        -- address details
        address1 as address_line_1,
        city,
        state,
        zip_code,
        country_code

    from source
)

select * from renamed