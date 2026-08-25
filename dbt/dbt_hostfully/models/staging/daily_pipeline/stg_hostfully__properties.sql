with source as (
    select * from {{ source('hostfully_api', 'raw_properties') }}
),

renamed as (
    select
        -- ids
        uid as property_id,
        agency_uid as agency_id,

        -- property details
        name as internal_property_name,
        property_type as property_type,
        listing_type as listing_type,
        room_type as room_type,
        business_type as business_type,
        is_active as is_active,
        
        -- physical attributes (area is JSON with size and unitType)
        cast(json_value(area, '$.size') as numeric) as size_sqm,
        json_value(area, '$.unitType') as size_unit_type,
        bedrooms as bedrooms_count,
        beds as beds_count,
        cast(bathrooms as numeric) as bathrooms_count,
        number_of_floors as number_of_floors,
        
        -- location (extracted from address JSON)
        json_value(address, '$.address') as street_address,
        json_value(address, '$.address2') as address_line_2,
        json_value(address, '$.city') as city,
        json_value(address, '$.state') as state,
        json_value(address, '$.zipCode') as zip_code,
        json_value(address, '$.countryCode') as country_code,
        cast(json_value(address, '$.latitude') as numeric) as latitude,
        cast(json_value(address, '$.longitude') as numeric) as longitude,
        
        picture_link as picture_url,

        -- availability JSON fields
        json_value(availability, '$.airbnbBookingStrategy') as airbnb_booking_strategy,
        cast(json_value(availability, '$.allowBookingRequestWhenOutOfLeadTime') as bool) as allow_booking_request_out_of_lead_time,
        cast(json_value(availability, '$.baseGuests') as int64) as base_guests,
        json_value(availability, '$.bookingDotComBookingStrategy') as booking_dot_com_booking_strategy,

        -- pricing JSON fields
        cast(json_value(pricing, '$.cleaningFee') as numeric) as pricing_cleaning_fee,
        cast(json_value(pricing, '$.cleaningFeeTaxRate') as numeric) as pricing_cleaning_fee_tax_rate,
        json_value(pricing, '$.currency') as pricing_currency,
        cast(json_value(pricing, '$.dailyRate') as numeric) as pricing_daily_rate,
        cast(json_value(pricing, '$.extraGuestFee') as numeric) as pricing_extra_guest_fee,
        json_value(pricing, '$.fullPaymentTiming') as pricing_full_payment_timing,
        cast(json_value(pricing, '$.daysOverWhichTaxRateChargeShouldBeIgnored') as int64) as pricing_days_over_which_tax_rate_charge_ignored,

        -- booking_dot_com_data JSON fields
        cast(json_value(booking_dot_com_data, '$.active') as bool) as booking_dot_com_active,
        cast(json_value(booking_dot_com_data, '$.activeForAgency') as bool) as booking_dot_com_active_for_agency,
        json_value(booking_dot_com_data, '$.cancellationPolicyCode') as booking_dot_com_cancellation_policy_code,
        json_value(booking_dot_com_data, '$.contactEmployeeUid') as booking_dot_com_contact_employee_uid,
        json_value(booking_dot_com_data, '$.roomName') as booking_dot_com_room_name,

        -- hvmi_data JSON fields
        cast(json_value(hvmi_data, '$.active') as bool) as hvmi_active,
        cast(json_value(hvmi_data, '$.activeForAgency') as bool) as hvmi_active_for_agency,

        -- add updated_at_utc for snapshot change tracking
        current_timestamp() as updated_at_utc

    from source
)

select * from renamed