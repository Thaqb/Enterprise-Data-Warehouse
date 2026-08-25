{% snapshot snp_dim_properties %}

{{
    config(
      target_database='cloud-data-pipeline-projects',
      target_schema='snapshots',
      unique_key='property_id',

      strategy='check',
      check_cols='all',
    )
}}

select * from {{ ref('dim_properties') }}

{% endsnapshot %}
