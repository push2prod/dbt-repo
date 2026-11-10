{{ config(tags=['customers']) }}
select (select count(*) from {{ ref('dim_customers_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_customers_daily') }}) as total_amount
