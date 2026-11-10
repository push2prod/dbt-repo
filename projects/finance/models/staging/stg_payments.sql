select id as payment_id, order_id, method, {{ shared_macros.cents_to_dollars('amount_cents') }} as amount, paid_at::date as paid_at
from {{ ref('raw_payments') }}
