{{ config(tags=['procurement']) }}
select e.*, n.first_seen_at
from {{ ref('stg_procurement_events') }} e
join {{ ref('stg_procurement_entities') }} n using (entity_id)
