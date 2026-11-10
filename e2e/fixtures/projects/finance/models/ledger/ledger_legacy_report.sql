{{ config(tags=['ledger', 'deprecated']) }}
select * from {{ ref('fct_ledger_daily') }}
