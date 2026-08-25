with source as (
    select * from {{ source('hostfully_api', 'raw_properties') }}
    -- only include rows that actually have an Airbnb ID
    where json_value(airbnb_data, '$.airbnbId') is not null and trim(json_value(airbnb_data, '$.airbnbId')) != ''
),
aggregated_reviews as (
    select property_id, round(avg(overall_rating), 2) as average_review_score,
           count(review_id) as total_reviews 
    from {{ ref('stg_hostfully__property_reviews_airbnb') }}
    group by property_id
),

renamed as (
    select
        -- ids
        json_value(airbnb_data, '$.airbnbId') as airbnb_id,
        uid as property_id,
        -- status fields
        cast(json_value(airbnb_data, '$.active') as boolean) as is_active_for_airbnb,

        -- aggregated reviews
        ar.average_review_score,
        coalesce(ar.total_reviews, 0) as total_reviews_count
    from source
    left join aggregated_reviews ar on source.uid = ar.property_id
)

select * from renamed