{{ config(tags=['billing']) }}
select (select count(*) from {{ ref('dim_billing_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_billing_daily') }}) as total_amount
