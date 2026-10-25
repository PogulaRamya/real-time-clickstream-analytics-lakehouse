{{ config(materialized='incremental', unique_key=['window_start','product_id'], incremental_strategy='merge') }}
select window_start, product_id, views, carts, purchases, revenue_cents, updated_at
from {{ source('raw', 'product_minute') }}
{% if is_incremental() %}
where updated_at >= (select dateadd(minute, -10, coalesce(max(updated_at), '1970-01-01'::timestamp_ntz)) from {{ this }})
{% endif %}
