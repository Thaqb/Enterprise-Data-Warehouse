with reviews_source as (
    select * from {{ source('hostfully_api', 'raw_property_reviews_booking') }}
),

renamed as (
    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key(['property_uid']) }} as review_id,
        
        -- foreign keys
        property_uid as property_id,
        
        -- review scores (all as JSON objects with areaAverageScore, maxScore, score fields)
        reply_score,
        content_score,
        review_score,
        quality_rating_score,
        work_friendly_score,
        
        -- extract actual score values from JSON
        cast(json_value(reply_score, '$.score') as numeric) as reply_score_value,
        cast(json_value(content_score, '$.score') as numeric) as content_score_value,
        cast(json_value(review_score, '$.score') as numeric) as review_score_value,
        cast(json_value(quality_rating_score, '$.score') as numeric) as quality_rating_score_value,
        cast(json_value(work_friendly_score, '$.score') as numeric) as work_friendly_score_value,
        
        -- extract max score for reference
        cast(json_value(content_score, '$.maxScore') as numeric) as max_score,
        
        -- metadata
        _dlt_load_id,
        current_timestamp() as updated_at_utc
        
    from reviews_source
)

select * from renamed
