with reviews_source as (
    select * from {{ ref('stg_hostfully__property_reviews_airbnb') }}
),

final as (
    select
        review_id,
        property_id,
        lead_id,

        -- Review Date Attributes
        review_date,
        extract(year from review_date) as review_year,
        extract(month from review_date) as review_month,
        format_date('%B %Y', review_date) as review_month_year,

        -- Author & Content
        author_name,
        -- review_title,
        review_content,
        private_feedback,
        review_comments as sub_category_comments,

        -- Ratings Logic
        overall_rating,
        rating_accuracy,
        rating_cleanliness,
        rating_checkin,
        rating_value,
        rating_location,
        rating_communication,

        -- Sentiment/Quality Flags
        case when overall_rating >= 5 then true else false end as is_perfect_review,
        case when overall_rating <= 3 then true else false end as is_negative_review,
        length(review_content) as review_length_chars,

        -- Metadata
        updated_at_utc

    from reviews_source
)

select * from final