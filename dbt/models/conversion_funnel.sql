select date_trunc('day', session_start) as session_date,
       count(*) as total_sessions,
       count_if(funnel_stage >= 1) as product_view_sessions,
       count_if(funnel_stage >= 2) as cart_sessions,
       count_if(funnel_stage >= 3) as checkout_sessions,
       count_if(funnel_stage >= 4) as purchase_sessions,
       count_if(funnel_stage >= 4)::float / nullif(count_if(funnel_stage >= 1),0) as conversion_rate,
       max(updated_at) as last_updated_at
from {{ ref('fct_sessions') }}
group by 1
