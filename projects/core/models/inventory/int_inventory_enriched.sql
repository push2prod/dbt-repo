{{ config(tags=['inventory', 'daily']) }}
select e.*, n.first_seen_at
from {{ ref('stg_inventory_events') }} e
join {{ ref('stg_inventory_entities') }} n using (entity_id)
