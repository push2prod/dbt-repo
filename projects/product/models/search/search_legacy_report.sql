{{ config(tags=['search', 'deprecated']) }}
select * from {{ ref('fct_search_daily') }}
