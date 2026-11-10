{{ config(tags=['content', 'daily']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_content_enriched') }}
group by 1
