{{ config(tags=['daily', 'deprecated']) }}
select * from {{ ref('fct_revenue') }}
