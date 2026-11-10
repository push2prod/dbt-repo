{{ config(tags=['orders']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_orders_enriched') }}
group by 1
