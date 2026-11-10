{{ config(tags=['monthly']) }}
select date_trunc('month', issued_at) as month, sum(amount) as invoiced from {{ ref('stg_invoices') }} group by 1
