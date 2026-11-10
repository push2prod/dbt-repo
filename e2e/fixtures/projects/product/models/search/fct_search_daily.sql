{{ config(tags=['search']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_search_enriched') }}
group by 1
