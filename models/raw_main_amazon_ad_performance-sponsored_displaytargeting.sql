{{ config(enabled=var('amazon_ads__displaytargeting_enabled', True)) }}
{{
config(
	alias=var('amazon_ads__sponsored_displaytargeting_alias','amazon-ad_performance-sponsored_displaytargeting-v1' ),
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
    from {{ source('dbt_amazon_ads', 'sd_target_report') }}
),

campaign as (
    select
        id,
        name,
        profile_id,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sd_campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

adgroup as (
    select
        id,
        name,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sd_ad_group_history') }}
),

fields as (
    select
        "TARGETING_EXPRESSION" as targetingtype,
        safe_cast(report.video_complete_views as STRING) as videocompleteviews,
        safe_cast(report.video_unmutes as STRING) as videounmutes,
        safe_cast(campaign.name as STRING) as campaignname,
        safe_cast(report.ad_group_id as STRING) as adgroupid,
        safe_cast(report.video_third_quartile_views as STRING)
            as videothirdquartileviews,
        safe_cast(report.targeting_text as STRING) as targetingtext,
        safe_cast(profile.country_code as STRING) as profilecountrycode,
        safe_cast(profile.id as STRING) as profileid,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.video_midpoint_views as STRING) as videomidpointviews,
        safe_cast(campaign.id as STRING) as campaignid,
        safe_cast(profile.account_name as STRING) as profilebrandname,
        safe_cast(report.video_first_quartile_views as STRING)
            as videofirstquartileviews,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(adgroup.name as STRING) as adgroupname,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(report.date as DATE) as date,
        safe_cast(report.purchases_promoted_clicks as STRING)
            as purchasespromotedclicks,
        safe_cast(report.impressions_views as STRING) as impressionsviews,
        safe_cast(report.sales_clicks as STRING) as salesclicks,
        safe_cast(report.purchases_clicks as STRING) as purchasesclicks,
        safe_cast(report.new_to_brand_sales_clicks as STRING)
            as newtobrandsalesclicks,
        safe_cast(report.campaign_budget_currency_code as STRING)
            as campaignbudgetcurrencycode,
        safe_cast(report.sales_promoted_clicks as STRING)
            as salespromotedclicks,
        safe_cast(report.new_to_brand_purchases_clicks as STRING)
            as newtobrandpurchasesclicks,
        safe_cast(report.new_to_brand_units_sold_clicks as STRING)
            as newtobrandunitssoldclicks,
        safe_cast(null as STRING) as attributedconversions14d,
        safe_cast(null as STRING) as attributedconversions14dsamesku,
        safe_cast(null as STRING) as attributedconversions1d,
        safe_cast(null as STRING) as attributedconversions1dsamesku,
        safe_cast(null as STRING) as attributedconversions30d,
        safe_cast(null as STRING) as attributedconversions30dsamesku,
        safe_cast(null as STRING) as attributedconversions7d,
        safe_cast(null as STRING) as attributedconversions7dsamesku,
        safe_cast(null as STRING) as attributedordersnewtobrand14d,
        safe_cast(null as STRING) as attributedsales14d,
        safe_cast(null as STRING) as attributedsales14dsamesku,
        safe_cast(null as STRING) as attributedsales1d,
        safe_cast(null as STRING) as attributedsales1dsamesku,
        safe_cast(null as STRING) as attributedsales30d,
        safe_cast(null as STRING) as attributedsales30dsamesku,
        safe_cast(null as STRING) as attributedsales7d,
        safe_cast(null as STRING) as attributedsales7dsamesku,
        safe_cast(null as STRING) as attributedsalesnewtobrand14d,
        safe_cast(null as STRING) as attributedunitsordered1d,
        safe_cast(null as STRING) as attributedunitsordered14d,
        safe_cast(null as STRING) as attributedunitsordered30d,
        safe_cast(null as STRING) as attributedunitsordered7d,
        safe_cast(null as STRING) as attributedunitsorderednewtobrand14d,
        safe_cast(null as STRING) as currency,
        safe_cast(null as STRING) as viewimpressions



    from report
    left join campaign
        on report.campaign_id = campaign.id and campaign.is_most_recent_record
    left join profile
        on profile.id = safe_cast(campaign.profile_id as INT64)
    left join adgroup
        on report.ad_group_id = adgroup.id and adgroup.is_most_recent_record








)

select * from fields
