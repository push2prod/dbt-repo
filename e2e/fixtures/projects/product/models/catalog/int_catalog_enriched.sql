{{ config(tags=['catalog']) }}
select e.*, n.first_seen_at
from {{ ref('stg_catalog_events') }} e
join {{ ref('stg_catalog_entities') }} n using (entity_id)
