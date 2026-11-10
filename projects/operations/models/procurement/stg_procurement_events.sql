{{ config(tags=['procurement']) }}
select id as event_id, entity_id, {{ shared_macros.cents_to_dollars('amount_cents') }} as amount, created_at::date as created_at
from {{ ref('raw_procurement') }}
