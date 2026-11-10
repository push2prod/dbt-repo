{{ config(tags=['orders']) }}
select e.*, n.first_seen_at
from {{ ref('stg_orders_events') }} e
join {{ ref('stg_orders_entities') }} n using (entity_id)
