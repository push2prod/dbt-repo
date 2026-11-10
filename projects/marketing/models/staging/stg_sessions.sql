select id as session_id, campaign_id, converted, started_at::timestamp as started_at
from {{ ref('raw_sessions') }}
