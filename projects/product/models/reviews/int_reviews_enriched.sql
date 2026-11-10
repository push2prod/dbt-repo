{{ config(tags=['reviews']) }}
select e.*, n.first_seen_at
from {{ ref('stg_reviews_events') }} e
join {{ ref('stg_reviews_entities') }} n using (entity_id)
