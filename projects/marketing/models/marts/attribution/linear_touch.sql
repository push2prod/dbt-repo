{{ config(tags=['hourly']) }}
select campaign_id, count(*) as credit from {{ ref('stg_sessions') }} group by 1
