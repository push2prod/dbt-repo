{{ config(tags=['customers', 'deprecated']) }}
select * from {{ ref('fct_customers_daily') }}
