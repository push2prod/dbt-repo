{{ config(tags=['procurement']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_procurement_enriched') }}
group by 1
