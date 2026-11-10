{{ config(tags=['search']) }}
select e.*, n.first_seen_at
from {{ ref('stg_search_events') }} e
join {{ ref('stg_search_entities') }} n using (entity_id)
