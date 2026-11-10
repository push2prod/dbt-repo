{{ config(tags=['attribution', 'deprecated']) }}
select * from {{ ref('fct_attribution_daily') }}
