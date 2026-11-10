{{ config(materialized='view', tags=['shipping']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_shipping') }}
group by 1
