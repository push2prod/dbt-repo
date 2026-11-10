{{ config(tags=['shipping']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_shipping_enriched') }}
group by 1
