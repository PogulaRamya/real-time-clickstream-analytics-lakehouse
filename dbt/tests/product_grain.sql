select window_start, product_id from {{ ref('fct_product_minute') }}
group by 1,2 having count(*) > 1
