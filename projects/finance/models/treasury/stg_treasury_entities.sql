{{ config(materialized='view', tags=['treasury']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_treasury') }}
group by 1
