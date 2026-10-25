select * from {{ ref('fct_product_minute') }}
where views < 0 or carts < 0 or purchases < 0 or revenue_cents < 0
