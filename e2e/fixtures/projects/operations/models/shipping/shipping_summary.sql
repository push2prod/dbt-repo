{{ config(tags=['shipping']) }}
select (select count(*) from {{ ref('dim_shipping_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_shipping_daily') }}) as total_amount
