with rules as (
    select * from {{ ref('stg_hostfully__property_rules') }}
    where is_airbnb_applicable is not null
       or is_booking_dot_com_applicable is not null
       or is_google_applicable is not null
),

pivoted as (
    select
        property_id,
        
        -- POSITIVE RULES (If true in API, true in Table)
        max(case when rule_name = 'ALLOWS_SMOKING' then true else false end) as is_smoking_allowed,
        max(case when rule_name = 'ALLOWS_PETS' then true else false end) as is_pet_friendly,
        max(case when rule_name = 'IS_EVENT_FRIENDLY' then true else false end) as is_event_friendly,
        max(case when rule_name = 'IS_FAMILY_FRIENDLY' then true else false end) as is_family_friendly,

        -- NEGATIVE RULES (If true in API, must be FALSE in Table)
        -- Logic: If the 'NOT_ALLOWED' rule is active, friendly = false. 
        -- Otherwise (if the rule is missing or false), friendly = true.
        min(case when rule_name = 'CHILDREN_NOT_ALLOWED' then false else true end) as is_children_friendly,
        min(case when rule_name = 'INFANT_NOT_ALLOWED' then false else true end) as is_infant_friendly,

        
        -- Channel Audit Metrics (Counting only 'true' values)
        countif(is_airbnb_applicable is true) as airbnb_rule_count,
        countif(is_booking_dot_com_applicable is true) as booking_com_rule_count,
        countif(is_google_applicable is true) as google_rule_count,
        -- Logic to check if rules are consistent across all active channels
        logical_and(
            coalesce(is_airbnb_applicable, false) = coalesce(is_booking_dot_com_applicable, false)
        ) as is_channel_sync_consistent
    from rules
    group by property_id
)
select * from pivoted