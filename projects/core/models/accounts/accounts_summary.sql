{{ config(tags=['accounts']) }}
select (select count(*) from {{ ref('dim_accounts_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_accounts_daily') }}) as total_amount
