{{ config(tags=['procurement', 'deprecated']) }}
select * from {{ ref('fct_procurement_daily') }}
