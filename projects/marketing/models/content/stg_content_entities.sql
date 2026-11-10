{{ config(materialized='view', tags=['content']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_content') }}
group by 1
