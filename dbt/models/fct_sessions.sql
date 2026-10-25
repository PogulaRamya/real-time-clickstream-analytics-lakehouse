{{ config(materialized='incremental', unique_key='session_id', incremental_strategy='merge') }}
select session_id, session_start, session_close, event_count, revenue_cents, funnel_stage, updated_at
from {{ source('raw', 'sessions') }}
{% if is_incremental() %}
where updated_at >= (select dateadd(minute, -10, coalesce(max(updated_at), '1970-01-01'::timestamp_ntz)) from {{ this }})
{% endif %}
