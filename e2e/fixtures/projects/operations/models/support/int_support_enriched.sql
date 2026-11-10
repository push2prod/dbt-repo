{{ config(tags=['support']) }}
select e.*, n.first_seen_at
from {{ ref('stg_support_events') }} e
join {{ ref('stg_support_entities') }} n using (entity_id)
