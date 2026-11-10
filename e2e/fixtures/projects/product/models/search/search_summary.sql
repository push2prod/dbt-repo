{{ config(tags=['search']) }}
select (select count(*) from {{ ref('dim_search_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_search_daily') }}) as total_amount
