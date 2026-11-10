{{ config(materialized='view', tags=['audiences']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_audiences') }}
group by 1
