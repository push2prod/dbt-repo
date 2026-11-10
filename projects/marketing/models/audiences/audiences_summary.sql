{{ config(tags=['audiences']) }}
select (select count(*) from {{ ref('dim_audiences_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_audiences_daily') }}) as total_amount
