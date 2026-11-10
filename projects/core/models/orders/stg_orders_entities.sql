{{ config(materialized='view', tags=['orders']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_orders') }}
group by 1
