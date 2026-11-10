{{ config(tags=['treasury', 'daily']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_treasury_enriched') }}
group by 1
