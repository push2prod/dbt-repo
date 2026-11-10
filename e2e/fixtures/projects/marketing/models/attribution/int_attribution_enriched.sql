{{ config(tags=['attribution']) }}
select e.*, n.first_seen_at
from {{ ref('stg_attribution_events') }} e
join {{ ref('stg_attribution_entities') }} n using (entity_id)
