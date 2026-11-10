{{ config(tags=['billing']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_billing_enriched') }}
group by 1
