{{ config(tags=['treasury']) }}
select (select count(*) from {{ ref('dim_treasury_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_treasury_daily') }}) as total_amount
