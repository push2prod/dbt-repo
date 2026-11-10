{{ config(tags=['catalog']) }}
select (select count(*) from {{ ref('dim_catalog_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_catalog_daily') }}) as total_amount
