{{ config(tags=['campaigns']) }}
select e.*, n.first_seen_at
from {{ ref('stg_campaigns_events') }} e
join {{ ref('stg_campaigns_entities') }} n using (entity_id)
