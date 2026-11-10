{{ config(tags=['campaigns']) }}
select (select count(*) from {{ ref('dim_campaigns_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_campaigns_daily') }}) as total_amount
