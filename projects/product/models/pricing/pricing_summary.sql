{{ config(tags=['pricing']) }}
select (select count(*) from {{ ref('dim_pricing_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_pricing_daily') }}) as total_amount
