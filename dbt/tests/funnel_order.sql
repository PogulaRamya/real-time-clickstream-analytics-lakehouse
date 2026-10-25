select * from {{ ref('conversion_funnel') }}
where product_view_sessions > total_sessions or cart_sessions > product_view_sessions
   or checkout_sessions > cart_sessions or purchase_sessions > checkout_sessions
