{{ config(tags=['support']) }}
select (select count(*) from {{ ref('dim_support_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_support_daily') }}) as total_amount
