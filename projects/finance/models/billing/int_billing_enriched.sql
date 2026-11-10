{{ config(tags=['billing', 'daily']) }}
select e.*, n.first_seen_at
from {{ ref('stg_billing_events') }} e
join {{ ref('stg_billing_entities') }} n using (entity_id)
