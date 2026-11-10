{{ config(materialized='view', tags=['billing']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_billing') }}
group by 1
