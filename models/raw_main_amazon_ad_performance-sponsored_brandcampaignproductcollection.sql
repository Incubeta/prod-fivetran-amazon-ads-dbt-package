{{ config(enabled=var('amazon_ads__brandcampaignproductcollection_enabled', True)) }}
{{
config(
	alias=var('amazon_ads__sponsored_brandcampaignproductcollection_alias','amazon-ad_performance-sponsored_brandcampaignproductcollection-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
	materialized='incremental',
	incremental_strategy='insert_overwrite'
  )

}}


with report as (
    select *
    from {{ source('dbt_amazon_ads', 'sb_placement_report') }}
),

campaign as (
    select * from {{ source('dbt_amazon_ads', 'sb_campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        campaign.name as campaignname,
        report.report_date as date,
        campaign.id as campaignid,
        campaign.state as campaignstatus,
        report.placement,
        profile.country_code as profilecountrycode,
        campaign.profile_id as profileid,
        SAFE_CAST(report.currency as STRING) as currency,
        SAFE_CAST(report.attributed_sales_14_d as STRING) as attributedsales14d,
        SAFE_CAST(report.attributed_conversions_14_d as STRING)
            as attributedconversions14d,
        SAFE_CAST(report.clicks as STRING) as clicks,
        SAFE_CAST(report.cost as STRING) as cost,
        SAFE_CAST(report.impressions as STRING) as impressions,

        SAFE_CAST(report.units_sold_14_d as STRING) as unitssold14d

    from report
    left join campaign
        on report.campaign_id = campaign.id
    left join profile
        on campaign.profile_id = profile.id








)

select * from fields
