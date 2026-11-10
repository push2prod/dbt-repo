{{ config(tags=['treasury']) }}
select e.*, n.first_seen_at
from {{ ref('stg_treasury_events') }} e
join {{ ref('stg_treasury_entities') }} n using (entity_id)
