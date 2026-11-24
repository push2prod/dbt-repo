{{ config(tags=["daily"]) }}
-- A stand-in model: no source tables needed.
select 1 as order_id, 100 as amount_cents
