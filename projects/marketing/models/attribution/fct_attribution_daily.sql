{{ config(tags=['attribution']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_attribution_enriched') }}
group by 1
