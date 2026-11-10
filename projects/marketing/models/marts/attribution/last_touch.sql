{{ config(tags=['hourly']) }}
select campaign_id, max(started_at) as last_touch_at from {{ ref('stg_sessions') }} group by 1
