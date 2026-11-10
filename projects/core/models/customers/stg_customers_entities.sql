{{ config(materialized='view', tags=['customers']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_customers') }}
group by 1
