{{ config(materialized='view', tags=['support']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_support') }}
group by 1
