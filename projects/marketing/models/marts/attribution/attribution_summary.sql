{{ config(tags=['hourly']) }}
select f.campaign_id, l.credit as linear_credit, t.credit as decay_credit, p.credit as position_credit
from {{ ref('first_touch') }} f
join {{ ref('linear_touch') }} l using (campaign_id)
join {{ ref('time_decay') }} t using (campaign_id)
join {{ ref('position_based') }} p using (campaign_id)
