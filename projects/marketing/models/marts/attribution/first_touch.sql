{{ config(tags=['hourly']) }}
select campaign_id, min(started_at) as first_touch_at from {{ ref('stg_sessions') }} group by 1
