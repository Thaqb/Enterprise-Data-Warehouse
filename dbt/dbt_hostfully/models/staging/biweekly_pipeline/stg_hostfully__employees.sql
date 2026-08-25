with source as (
    select * from {{ source('hostfully_api', 'raw_employees') }}
),

renamed as (
    select
        -- ids
        uid as employee_id,
        agency_uid as agency_id,

        -- employee details
        CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) as full_name,
        email,
        role as employee_role,
        phone_number,
        phone_area_code

    from source
)

select * from renamed