{{ config(tags=['audiences', 'daily']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_audiences_enriched') }}
group by 1
