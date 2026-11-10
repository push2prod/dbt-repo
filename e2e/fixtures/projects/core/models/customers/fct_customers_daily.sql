{{ config(tags=['customers']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_customers_enriched') }}
group by 1
