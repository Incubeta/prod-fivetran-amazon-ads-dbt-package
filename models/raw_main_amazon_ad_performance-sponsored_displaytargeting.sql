{{ config(enabled=var('amazon_ads__displaytargeting_enabled', True))}}
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
	select id,name,profile_id, row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
adgroup as (
	select id,name,row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'sd_ad_group_history')}}
),
fields as (
	select 
    SAFE_CAST( report.video_complete_views AS STRING ) videoCompleteViews,
    SAFE_CAST( report.video_unmutes AS STRING ) videoUnmutes,
    SAFE_CAST( campaign.name AS STRING ) campaignName,
    SAFE_CAST( report.ad_group_id AS STRING ) adGroupId,
    SAFE_CAST( report.video_third_quartile_views AS STRING ) videoThirdQuartileViews,
    SAFE_CAST( report.targeting_text AS STRING ) targetingText,
    SAFE_CAST( profile.country_code AS STRING ) profileCountryCode,
    SAFE_CAST( profile.id AS STRING ) profileId,
    SAFE_CAST( report.clicks AS STRING ) clicks,
    SAFE_CAST( report.video_midpoint_views AS STRING ) videoMidpointViews,
    SAFE_CAST( campaign.id AS STRING ) campaignId,
    SAFE_CAST( profile.account_name AS STRING ) profileBrandName,
    SAFE_CAST( report.video_first_quartile_views AS STRING ) videoFirstQuartileViews,
    SAFE_CAST( report.impressions AS STRING ) impressions,
    SAFE_CAST( adgroup.name AS STRING ) adGroupName,
    SAFE_CAST( report.cost AS STRING ) cost,
    SAFE_CAST( report.date AS DATE ) date,
SAFE_CAST(report.purchases_promoted_clicks as STRING) purchasesPromotedClicks,
SAFE_CAST(report.impressions_views as STRING) impressionsViews,
SAFE_CAST(report.sales_clicks as STRING) salesClicks,
SAFE_CAST(report.purchases_clicks as STRING) purchasesClicks,
SAFE_CAST(report.new_to_brand_sales_clicks as STRING) newToBrandSalesClicks,
SAFE_CAST(report.campaign_budget_currency_code as STRING) campaignBudgetCurrencyCode,
SAFE_CAST(report.sales_promoted_clicks as STRING) salesPromotedClicks,
SAFE_CAST(report.new_to_brand_purchases_clicks as STRING) newToBrandPurchasesClicks,
SAFE_CAST(report.new_to_brand_units_sold_clicks as STRING) newToBrandUnitsSoldClicks,
	null as attributedConversions14d,
	null as attributedConversions14dSameSKU,
	null as attributedConversions1d,
	null as attributedConversions1dSameSKU,
	null as attributedConversions30d,
	null as attributedConversions30dSameSKU,
	null as attributedConversions7d,
	null as attributedConversions7dSameSKU,
	null as attributedOrdersNewToBrand14d,
	null as attributedSales14d,
	null as attributedSales14dSameSKU,
	null as attributedSales1d,
	null as attributedSales1dSameSKU,
	null as attributedSales30d,
	null as attributedSales30dSameSKU,
	null as attributedSales7d,
	null as attributedSales7dSameSKU,
	null as attributedSalesNewToBrand14d,
	null as attributedUnitsOrdered1d,
	null as attributedUnitsOrdered30d,
	null as attributedUnitsOrdered7d,
	null as attributedUnitsOrderedNewToBrand14d,
	null as currency,
	null as viewImpressions



		from report
		left join campaign
			on campaign.id = report.campaign_id and campaign.is_most_recent_record
		left join profile
			on profile.id = SAFE_CAST(campaign.profile_id as INT64)
		left join adgroup
			on report.ad_group_id = adgroup.id and adgroup.is_most_recent_record








)

select * from fields
