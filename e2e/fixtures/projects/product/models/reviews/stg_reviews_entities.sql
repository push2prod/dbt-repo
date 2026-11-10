{{ config(materialized='view', tags=['reviews']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_reviews') }}
group by 1
