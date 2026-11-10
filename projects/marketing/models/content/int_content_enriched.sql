{{ config(tags=['content']) }}
select e.*, n.first_seen_at
from {{ ref('stg_content_events') }} e
join {{ ref('stg_content_entities') }} n using (entity_id)
