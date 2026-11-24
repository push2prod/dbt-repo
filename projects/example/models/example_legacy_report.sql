{{ config(tags=["daily", "deprecated"]) }}
-- Tagged "deprecated": the example DAG excludes it.
select * from {{ ref('example_order_totals') }}
