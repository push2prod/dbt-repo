{{ config(tags=['treasury']) }}
select distinct entity_id, min(created_at::date) over (partition by entity_id) as first_seen_at
from {{ ref('raw_treasury') }}
