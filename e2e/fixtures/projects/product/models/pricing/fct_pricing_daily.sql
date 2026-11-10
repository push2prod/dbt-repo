{{ config(tags=['pricing']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_pricing_enriched') }}
group by 1
