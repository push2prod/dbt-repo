{{ config(materialized='view', tags=['procurement']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_procurement') }}
group by 1
