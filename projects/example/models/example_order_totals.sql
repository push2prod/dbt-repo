{{ config(tags=["daily"]) }}
select count(*) as orders, sum(amount_cents) as amount_cents
from {{ ref('example_orders') }}
