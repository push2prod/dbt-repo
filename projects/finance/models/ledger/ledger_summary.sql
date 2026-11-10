{{ config(tags=['ledger']) }}
select (select count(*) from {{ ref('dim_ledger_entity') }}) as entities,
       (select sum(amount) from {{ ref('fct_ledger_daily') }}) as total_amount
