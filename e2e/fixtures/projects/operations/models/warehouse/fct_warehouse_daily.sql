{{ config(tags=['warehouse']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_warehouse_enriched') }}
group by 1
