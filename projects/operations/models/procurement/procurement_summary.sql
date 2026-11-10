{{ config(tags=['procurement']) }}
select (select count(*) from {{ ref('dim_procurement_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_procurement_daily') }}) as total_amount
