select id as campaign_id, name, channel, {{ cents_to_dollars('spend_cents') }} as spend
from {{ ref('raw_campaigns') }}
