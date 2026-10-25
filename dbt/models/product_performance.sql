select date_trunc('day', window_start) as metric_date, product_id,
       sum(views) as views, sum(carts) as carts, sum(purchases) as purchase_events,
       sum(revenue_cents) / 100.0 as revenue_usd,
       sum(purchases)::float / nullif(sum(views),0) as purchases_per_view,
       max(updated_at) as last_updated_at
from {{ ref('fct_product_minute') }}
group by 1,2
