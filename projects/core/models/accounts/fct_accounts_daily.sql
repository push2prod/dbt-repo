{{ config(tags=['accounts', 'daily', 'hourly']) }}
select created_at as day, count(*) as events, sum(amount) as amount
from {{ ref('int_accounts_enriched') }}
group by 1
