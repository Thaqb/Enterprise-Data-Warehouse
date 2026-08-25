with status_source as (
    select * from {{ source('hostfully_api', 'raw_booking_dot_com_status') }}
),

renamed as (
    select
        -- primary key
        uid as property_uid,
        
        -- status information
        status as booking_dot_com_status,
        
        -- metadata
        _dlt_load_id,
        current_timestamp() as updated_at_utc
        
    from status_source
)

select * from renamed
