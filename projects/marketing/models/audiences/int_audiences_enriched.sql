{{ config(tags=['audiences']) }}
select e.*, n.first_seen_at
from {{ ref('stg_audiences_events') }} e
join {{ ref('stg_audiences_entities') }} n using (entity_id)
