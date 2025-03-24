with report as (
    select * from {{ ref('int_amazon_ad_performance-sponsored_producttargeting')}}
),
campaigns as (
    select * from {{ ref('stg_campaign_history') }}
),
adgroups as (
    select * from {{ ref('stg_ad_group_history') }}
),
profiles as (
    select * from {{ ref('stg_profile') }}
),
keyword_history as (
    select * from {{ ref('stg_keyword_history') }}
)

SELECT 
    profiles.profile_brand_name,
    profiles.profile_country_code,
    SAFE_CAST(profiles.id as STRING) as profile_id,

    campaigns.campaign_name,
    adgroups.adgroup_name,
    keyword_history.keyword_text as keyword,
    report.*,
    
    {{ add_fields("campaign_name") }} 

FROM report
LEFT JOIN campaigns
    on campaigns.campaign_id = report.campaign_id
LEFT JOIN adgroups
    on adgroups.adgroup_id = report.adgroup_id
LEFT JOIN profiles
    on profiles.id = campaigns.profile_id
LEFT JOIN keyword_history
    on keyword_history.id = report.keyword_id
