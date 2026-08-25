with settings_source as (
    select * from {{ source('hostfully_api', 'raw_property_booking_dot_com_settings') }}
),

renamed as (
    select
        -- primary key
        _properties_uid as property_uid,
        
        -- booking.com identifiers
        hotel_id as booking_dot_com_hotel_id,
        room_id as booking_dot_com_room_id,
        room_type,
        room_name,
        
        -- booking configuration
        booking_type,
        cast(cancellation_policy_code as string) as cancellation_policy_code,
        
        -- check-in methods (stored as JSON)
        primary_check_in_method,
        alternative_check_in_method,
        
        -- extract check-in method types
        json_value(primary_check_in_method, '$.type') as primary_check_in_method_type,
        json_value(primary_check_in_method, '$.brandName') as primary_check_in_brand,
        json_value(alternative_check_in_method, '$.type') as alternative_check_in_method_type,
        json_value(alternative_check_in_method, '$.when') as alternative_check_in_when,
        json_value(alternative_check_in_method, '$.how') as alternative_check_in_how,
        
        -- contact
        primary_contact_uid,
        
        -- metadata
        _dlt_load_id,
        current_timestamp() as updated_at_utc
        
    from settings_source
)

select * from renamed
