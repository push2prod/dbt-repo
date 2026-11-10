{{ config(tags=['hourly']) }}
select campaign_id, count(*) * 0.5 as credit from {{ ref('stg_sessions') }} group by 1
