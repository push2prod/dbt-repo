{{ config(tags=['payments']) }}
select e.*, n.first_seen_at
from {{ ref('stg_payments_events') }} e
join {{ ref('stg_payments_entities') }} n using (entity_id)
