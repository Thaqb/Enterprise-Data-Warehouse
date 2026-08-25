with source as (
    select * from {{ source('hostfully_api', 'raw_property_reviews_airbnb') }}
),

-- Unnest the JSON array and clean it up
flattened_categories as (
    select
        uid as review_id,
        coalesce(
            json_value(category_item, '$.category'), 
            'Communication'
        ) as category_name,
        cast(json_value(category_item, '$.rate') as int64) as category_rate,
        json_value(category_item, '$.comment') as category_comment
    from source,
    unnest(json_query_array(rating_categories)) as category_item
),

renamed as (
    select
        -- ids
        s.uid as review_id,
        s.property_uid as property_id,
        s.lead_uid as lead_id,

        -- review details
        cast(s.creation_date as date) as review_date,
        s.guest_name as author_name,
        cast(s.rating as int64) as overall_rating,
        -- s.source as review_source,
        -- s.title as review_title,
        s.public_review as review_content,
        s.private_note as private_feedback,
        
        -- aggregated sub-comments (Aggregated column - NOT in group by)
        string_agg(f.category_comment, '\n') as review_comments,

        -- flattened ratings (Aggregated columns - NOT in group by)
        max(case when f.category_name = 'ACCURACY' then f.category_rate end) as rating_accuracy,
        max(case when f.category_name = 'CLEANLINESS' then f.category_rate end) as rating_cleanliness,
        max(case when f.category_name = 'CHECKIN' then f.category_rate end) as rating_checkin,
        max(case when f.category_name = 'VALUE' then f.category_rate end) as rating_value,
        max(case when f.category_name = 'LOCATION' then f.category_rate end) as rating_location,
        max(case when f.category_name = 'Communication' then f.category_rate end) as rating_communication,

        -- metadata
        current_timestamp() as updated_at_utc

    from source s
    left join flattened_categories f on s.uid = f.review_id
    group by 
        review_id, 
        property_id, 
        lead_id, 
        review_date, 
        author_name, 
        overall_rating, 
        -- review_source, 
        -- review_title, 
        review_content, 
        private_feedback, 
        updated_at_utc
)

select * from renamed