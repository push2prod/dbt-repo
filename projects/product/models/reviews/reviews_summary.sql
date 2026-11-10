{{ config(tags=['reviews']) }}
select (select count(*) from {{ ref('dim_reviews_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_reviews_daily') }}) as total_amount
