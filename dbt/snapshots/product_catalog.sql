{% snapshot product_catalog_history %}
{{ config(target_schema='ANALYTICS', unique_key='product_id', strategy='check', check_cols=['product_name','category']) }}
select * from {{ ref('product_catalog') }}
{% endsnapshot %}
