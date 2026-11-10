{{ config(tags=['payments']) }}
select entity_id, first_seen_at, count(*) as events, sum(amount) as lifetime_amount
from {{ ref('int_payments_enriched') }}
group by 1, 2
