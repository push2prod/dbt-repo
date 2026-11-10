{{ config(materialized='view', tags=['campaigns']) }}
select entity_id, min(created_at::date) as first_seen_at
from {{ ref('raw_campaigns') }}
group by 1
