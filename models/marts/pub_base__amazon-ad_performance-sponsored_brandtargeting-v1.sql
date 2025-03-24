with report as (
    select * from {{ref('int_amazon_ad_performance-sponsored_brandtargeting')}}
),
keywords as (
    select * from {{ref('stg_sb_keyword')}}
),
campaigns as (
    select * from {{ref('stg_sb_campaigns')}}
),
adgroups as (
    select * from {{ref('stg_sb_ad_group_history')}}
),
profiles as (
    select * from {{ref('stg_profile')}}
)
SELECT 
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.campaign_type,
    campaigns.campaign_budget_type,
    profiles.profile_country_code,
    profiles.profile_brand_name,
    keywords.keyword_text,
    keywords.match_type,
    adgroups.adgroup_name,
    adgroups.adgroup_id,
    report.*,
    {{add_fields('campaigns.campaign_name')}}
from report
left join campaigns
    on report.campaign_id = campaigns.campaign_id
left join adgroups
    on report.ad_group_id = adgroups.adgroup_id
left join profiles
    on campaigns.profile_id = profiles.id
left join keywords
    on report.keyword_id = keywords.id

