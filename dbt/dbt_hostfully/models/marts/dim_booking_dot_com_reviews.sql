with reviews_source as (
    select * from {{ ref('stg_hostfully__property_reviews_booking') }}
),

final as (
    select
        review_id,
        property_id,
        
        -- Review Date Attributes
        -- Note: No date field available in current schema
        
        -- Review Scores (all on scale where maxScore = 100)
        review_score_value as overall_score,
        content_score_value as content_score,
        quality_rating_score_value as quality_rating_score,
        reply_score_value as reply_score,
        work_friendly_score_value as work_friendly_score,
        max_score,
        
        -- Calculate normalized percentage score
        round(safe_divide(review_score_value, max_score) * 100, 1) as overall_score_percentage,
        round(safe_divide(content_score_value, max_score) * 100, 1) as content_score_percentage,
        round(safe_divide(quality_rating_score_value, max_score) * 100, 1) as quality_rating_percentage,
        
        -- Sentiment/Quality Flags (using percentage thresholds)
        case when safe_divide(review_score_value, max_score) >= 0.9 then true else false end as is_excellent_review,
        case when safe_divide(review_score_value, max_score) <= 0.5 then true else false end as is_poor_review,
        
        -- Metadata
        updated_at_utc

    from reviews_source
)

select * from final
