{{ config(tags=['warehouse']) }}
select e.*, n.first_seen_at
from {{ ref('stg_warehouse_events') }} e
join {{ ref('stg_warehouse_entities') }} n using (entity_id)
