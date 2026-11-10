{{ config(tags=['ledger']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_ledger_enriched') }}
group by 1
