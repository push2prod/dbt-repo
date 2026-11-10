{{ config(materialized='view', tags=['catalog']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_catalog') }}
group by 1
