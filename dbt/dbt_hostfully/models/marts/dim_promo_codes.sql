select *
from {{ref('stg_hostfully__promo_codes')}}
order by promo_status