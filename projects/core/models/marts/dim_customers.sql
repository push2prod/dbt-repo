{{ config(tags=['daily']) }}
select c.customer_id, c.first_name, c.last_name, count(o.order_id) as order_count
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o using (customer_id)
group by 1, 2, 3
