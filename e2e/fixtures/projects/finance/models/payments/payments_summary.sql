{{ config(tags=['payments']) }}
select (select count(*) from {{ ref('dim_payments_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_payments_daily') }}) as total_amount
