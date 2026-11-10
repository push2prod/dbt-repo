{{ config(tags=['inventory']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_inventory_enriched') }}
group by 1
