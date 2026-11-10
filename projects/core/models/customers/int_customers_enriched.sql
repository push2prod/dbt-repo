{{ config(tags=['customers']) }}
select e.*, n.first_seen_at
from {{ ref('stg_customers_events') }} e
join {{ ref('stg_customers_entities') }} n using (entity_id)
