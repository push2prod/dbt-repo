{{ config(tags=['daily']) }}
select paid_at as day, sum(amount) as revenue from {{ ref('stg_payments') }} group by 1
