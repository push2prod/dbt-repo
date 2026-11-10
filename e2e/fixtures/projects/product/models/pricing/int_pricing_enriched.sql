{{ config(tags=['pricing']) }}
select e.*, n.first_seen_at
from {{ ref('stg_pricing_events') }} e
join {{ ref('stg_pricing_entities') }} n using (entity_id)
