{{ config(materialized='view', tags=['accounts']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_accounts') }}
group by 1
