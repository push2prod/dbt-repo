{{ config(tags=['content']) }}
select (select count(*) from {{ ref('dim_content_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_content_daily') }}) as total_amount
