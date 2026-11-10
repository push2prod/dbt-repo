{{ config(tags=['orders']) }}
select (select count(*) from {{ ref('dim_orders_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_orders_daily') }}) as total_amount
