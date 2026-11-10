{{ config(tags=['daily']) }}
select c.campaign_id, c.name, c.spend, count(s.session_id) as sessions,
       sum(case when s.converted then 1 else 0 end) as conversions
from {{ ref('stg_campaigns') }} c
left join {{ ref('stg_sessions') }} s using (campaign_id)
group by 1, 2, 3
