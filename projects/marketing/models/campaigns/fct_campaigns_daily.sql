{{ config(tags=['campaigns']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_campaigns_enriched') }}
group by 1
