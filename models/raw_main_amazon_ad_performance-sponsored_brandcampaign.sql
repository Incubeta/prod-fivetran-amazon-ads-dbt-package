{{ config(enabled=var('amazon_ads__brandcampaigns_enabled', True)) }}
{{
config(
	alias=var('amazon_ads__sponsored_brandcampaign_alias','amazon-ad_performance-sponsored_brandcampaign-v1' ),
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
    from {{ source('dbt_amazon_ads', 'sb_campaign_report') }}
),

campaigns as (
    select
        id,
        name,
        state,
        profile_id,
        row_number() over (
            partition by id
            order by last_update_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sb_campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        campaigns.name as campaignname,
        campaigns.state as campaignstatus,
        profile.account_name as profilebrandname,
        report.report_date as date,
        safe_cast(report.units_sold_14_d as STRING) as unitssold14d,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.currency as STRING) as currency,
        safe_cast(report.attributed_sales_14_d as STRING) as attributedsales14d,
        safe_cast(report.attributed_conversions_14_d as STRING)
            as attributedconversions14d,
        -- is this the correct brand name?
        safe_cast(campaigns.id as STRING) as campaignid,
        safe_cast(campaigns.profile_id as STRING) as profileid,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(profile.country_code as STRING) as profilecountrycode


    from report
    left join campaigns
        on report.campaign_id = campaigns.id and campaigns.is_most_recent_record
    left join profile
        on campaigns.profile_id = profile.id








)

select * from fields
