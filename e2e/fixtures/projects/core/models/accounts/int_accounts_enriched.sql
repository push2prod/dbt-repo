{{ config(tags=['accounts']) }}
select e.*, n.first_seen_at
from {{ ref('stg_accounts_events') }} e
join {{ ref('stg_accounts_entities') }} n using (entity_id)
