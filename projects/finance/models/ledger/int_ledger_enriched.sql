{{ config(tags=['ledger']) }}
select e.*, n.first_seen_at
from {{ ref('stg_ledger_events') }} e
join {{ ref('stg_ledger_entities') }} n using (entity_id)
