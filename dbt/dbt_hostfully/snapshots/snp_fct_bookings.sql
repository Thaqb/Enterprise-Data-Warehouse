{% snapshot snp_fct_bookings %}

{{
    config(
      target_database='cloud-data-pipeline-projects',
      target_schema='snapshots',
      unique_key='lead_id',

      strategy='timestamp',
      updated_at='lead_last_updated_at_utc',
    )
}}

select * from {{ ref('fct_bookings') }}

{% endsnapshot %}
