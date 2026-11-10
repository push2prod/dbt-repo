{{ config(tags=['inventory']) }}
select (select count(*) from {{ ref('dim_inventory_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_inventory_daily') }}) as total_amount
