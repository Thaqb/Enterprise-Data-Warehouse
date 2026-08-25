{% snapshot snp_fct_pricing %}

{{
    config(
      target_database='cloud-data-pipeline-projects',
      target_schema='snapshots',
      unique_key='property_calendar_id',

      strategy='check',
      check_cols='all',
    )
}}

select * from {{ ref('fct_pricing') }}

{% endsnapshot %}
