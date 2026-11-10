{{ config(tags=['support']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_support_enriched') }}
group by 1
