{{ config(materialized='view', tags=['inventory']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_inventory') }}
group by 1
