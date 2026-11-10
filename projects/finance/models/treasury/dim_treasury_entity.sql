{{ config(tags=['treasury', 'monthly']) }}
select entity_id, first_seen_at, count(*) as events, sum(amount) as lifetime_amount
from {{ ref('int_treasury_enriched') }}
group by 1, 2
