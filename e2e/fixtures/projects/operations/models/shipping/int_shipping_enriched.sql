{{ config(tags=['shipping']) }}
select e.*, n.first_seen_at
from {{ ref('stg_shipping_events') }} e
join {{ ref('stg_shipping_entities') }} n using (entity_id)
