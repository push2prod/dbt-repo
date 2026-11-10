{{ config(tags=['reviews']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_reviews_enriched') }}
group by 1
