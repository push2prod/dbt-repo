{{ config(materialized='view', tags=['payments']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_payments') }}
group by 1
