{{ config(tags=['catalog']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_catalog_enriched') }}
group by 1
