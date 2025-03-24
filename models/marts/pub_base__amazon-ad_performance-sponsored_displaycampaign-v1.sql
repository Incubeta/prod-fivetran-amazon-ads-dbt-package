with campaigns as (
    select * from {{ref('stg_sd_campaign_history') }}

),
profiles as (
    select * from {{ ref('stg_profile') }}
),
report as (
    select * from {{ref('int_amazon_ad_performance-sponsored_displaycampaign') }}
)
SELECT 
    campaigns.campaign_name,
    campaigns.campaign_status,
    profiles.profile_brand_name,
    profiles.profile_country_code,
    profiles.id as profile_id,
    report.*,
    {{add_fields('campaigns.campaign_name')}}
from report
left join campaigns
    on report.campaign_id = campaigns.campaign_id
left join profiles
    on campaigns.profile_id = profiles.id
