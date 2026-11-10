select id as customer_id, first_name, last_name, created_at::date as created_at
from {{ ref('raw_customers') }}
