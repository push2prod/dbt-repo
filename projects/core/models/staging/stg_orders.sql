select id as order_id, customer_id, {{ shared_macros.cents_to_dollars('amount_cents') }} as amount, ordered_at::date as ordered_at
from {{ ref('raw_orders') }}
