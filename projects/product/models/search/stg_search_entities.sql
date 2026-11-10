{{ config(materialized='view', tags=['search']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_search') }}
group by 1
