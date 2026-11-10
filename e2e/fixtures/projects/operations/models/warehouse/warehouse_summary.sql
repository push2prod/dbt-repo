{{ config(tags=['warehouse']) }}
select (select count(*) from {{ ref('dim_warehouse_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_warehouse_daily') }}) as total_amount
