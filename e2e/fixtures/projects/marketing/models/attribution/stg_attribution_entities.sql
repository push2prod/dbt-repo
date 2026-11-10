{{ config(materialized='view', tags=['attribution']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_attribution') }}
group by 1
