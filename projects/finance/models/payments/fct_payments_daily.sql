{{ config(tags=['payments']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_payments_enriched') }}
group by 1
