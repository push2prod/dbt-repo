{{ config(materialized='view', tags=['pricing']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_pricing') }}
group by 1
