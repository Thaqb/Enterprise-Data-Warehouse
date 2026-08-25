{% snapshot snp_hostfully__promo_codes %}

{{
    config(
      target_database='cloud-data-pipeline-projects',
      target_schema='snapshots',
      unique_key='promo_code_id',
      strategy='check',
      check_cols=['promo_status', 'discount_value', 'valid_to_utc'],
    )
}}

select * from {{ ref('stg_hostfully__promo_codes') }}

{% endsnapshot %}