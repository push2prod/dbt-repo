select id as invoice_id, customer_id, {{ shared_macros.cents_to_dollars('amount_cents') }} as amount, issued_at::date as issued_at
from {{ ref('raw_invoices') }}
