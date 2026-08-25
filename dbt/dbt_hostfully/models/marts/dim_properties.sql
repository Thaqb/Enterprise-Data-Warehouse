with properties as (
    select * from {{ ref('stg_hostfully__properties') }}
),
property_descriptions as (
    select * from {{ ref('stg_hostfully__property_descriptions') }}
    where desc_language = 'en_US'
),
owners as (
    select * from {{ ref('stg_hostfully__owners') }}
),

property_owners as (
    select * from {{ ref('stg_hostfully__property_owners') }}
),
airbnb as (
    select * from {{ ref('stg_hostfully__airbnb_data') }}
),

booking_com_status as (
    select * from {{ ref('stg_hostfully__booking_dot_com_status') }}
),

booking_com_settings as (
    select * from {{ ref('stg_hostfully__property_booking_dot_com_settings') }}
),
final as (
    select
        -- ids
        p.property_id,
        p.agency_id,
        
        -- names & types
        p.internal_property_name as internal_property_name,
        pd.public_property_name as public_property_name,
        p.property_type,
        
        -- Status & Pricing
        p.is_active as is_active_on_hostfully,
        coalesce(ab.is_active_for_airbnb, false) as is_active_on_airbnb,
        -- Channel Mapping - Booking.com
        bcs.booking_dot_com_status,
        bcset.primary_contact_uid as booking_dot_com_primary_contact_uid,
        bcset.booking_dot_com_hotel_id,
        bcset.booking_dot_com_room_id,
        bcset.room_type as booking_dot_com_room_type,
        bcset.room_name as booking_dot_com_room_name,
        bcset.booking_type as booking_dot_com_booking_type,

        -- Location
        p.street_address as address,
        p.address_line_2,
        p.city,
        p.state as governerate,
        p.country_code,
        p.zip_code,
        p.longitude,
        p.latitude,

        -- Aggregated Reviews
        ab.average_review_score as airbnb_average_review_score,
        ab.total_reviews_count as airbnb_total_reviews_count,

        -- Owner Info
        o.owner_id,
        concat(ifnull(o.first_name, ''), ' ', ifnull(o.last_name, '')) as owner_name,
        o.email as owner_email,
        o.phone_number as owner_phone_number,
        o.is_active as is_active_owner,
        concat(cast(o.owner_commission_rate as string), '%') as agency_commission_rate,

        -- Capacity
        p.bedrooms_count,
        p.beds_count,
        p.bathrooms_count,
        p.base_guests,
        p.number_of_floors,
        p.size_sqm,

        -- Pricing (from pricing JSON)
        p.pricing_daily_rate as base_daily_rate,
        p.pricing_currency as currency,
        p.pricing_cleaning_fee as cleaning_fee,
        p.pricing_extra_guest_fee as extra_guest_fee,

        -- Availability settings
        p.airbnb_booking_strategy,
        p.booking_dot_com_booking_strategy,
        p.allow_booking_request_out_of_lead_time

    from properties p
    left join property_descriptions pd on p.property_id = pd.property_id
    left join property_owners po on p.property_id = po.property_id
    left join owners o on po.owner_id = o.owner_id
    left join airbnb ab on p.property_id = ab.property_id
    left join booking_com_status bcs on p.property_id = bcs.property_uid
    left join booking_com_settings bcset on p.property_id = bcset.property_uid
)

select * from final