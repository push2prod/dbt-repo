{{ config(materialized='view', tags=['warehouse']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_warehouse') }}
group by 1
