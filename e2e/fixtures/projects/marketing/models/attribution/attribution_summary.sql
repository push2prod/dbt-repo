{{ config(tags=['attribution']) }}
select (select count(*) from {{ ref('dim_attribution_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_attribution_daily') }}) as total_amount
