{{ config(materialized='view', tags=['ledger']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_ledger') }}
group by 1
