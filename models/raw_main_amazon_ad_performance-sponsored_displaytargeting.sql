{{ config(enabled=var('amazon_ads__displaytargeting_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_displaytargeting_alias','amazon-ad_performance-sponsored_displaytargeting-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )

}}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'sd_target_report') }}
),

campaign as (
	select * from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
adgroup as (
	select * from {{ source('dbt_amazon_ads', 'sd_ad_group_history')}}
),
fields as (
	select 
    SAFE_CAST( report.attributed_sales_1_d_same_sku AS STRING ) attributedSales1dSameSku,
    SAFE_CAST( report.attributed_units_ordered_7_d AS STRING ) attributedUnitsOrdered7d,
    SAFE_CAST( report.attributed_conversions_7_d_same_sku as STRING ) attributedConversions7dSameSku,
    SAFE_CAST( report.video_complete_views AS STRING ) videoCompleteViews,
    SAFE_CAST( report.attributed_orders_new_to_brand_14_d AS STRING ) attributedOrdersNewToBrand14d,
    SAFE_CAST( report.video_unmutes AS STRING ) videoUnmutes,
    SAFE_CAST( campaign.name AS STRING ) campaignName,
    SAFE_CAST( report.attributed_conversions_14_d_same_sku AS STRING ) attributedConversions14dSameSku,
    SAFE_CAST( report.ad_group_id AS STRING ) adGroupId,
    SAFE_CAST( report.video_third_quartile_views AS STRING ) videoThirdQuartileViews,
    SAFE_CAST( report.attributed_sales_30_d_same_sku AS STRING ) attributedSales30dSameSku,
    SAFE_CAST( report.attributed_conversions_1_d_same_sku AS STRING ) attributedConversions1dSameSku,
    SAFE_CAST( report.attributed_sales_1_d AS STRING ) attributedSales1d,
    SAFE_CAST( report.targeting_text AS STRING ) targetingText,
    SAFE_CAST( profile.country_code AS STRING ) profileCountryCode,
    SAFE_CAST( profile.id AS STRING ) profileId,
    SAFE_CAST( report.attributed_sales_14_d AS STRING ) attributedSales14d,
    SAFE_CAST( report.clicks AS STRING ) clicks,
    SAFE_CAST( report.attributed_sales_14_d_same_sku AS STRING ) attributedSales14dSameSku,
    SAFE_CAST( report.attributed_conversions_7_d AS STRING ) attributedConversions7d,
    SAFE_CAST( report.attributed_units_ordered_1_d AS STRING ) attributedUnitsOrdered1d,
    SAFE_CAST( report.attributed_units_ordered_30_d AS STRING ) attributedUnitsOrdered30d,
    SAFE_CAST( report.video_midpoint_views AS STRING ) videoMidpointViews,
    SAFE_CAST( campaign.id AS STRING ) campaignId,
    SAFE_CAST( report.attributed_units_ordered_new_to_brand_14_d AS STRING ) attributedUnitsOrderedNewToBrand14d,
    SAFE_CAST( report.view_impressions AS STRING ) viewImpressions,
    SAFE_CAST( profile.account_name AS STRING ) profileBrandName,
    SAFE_CAST( report.video_first_quartile_views AS STRING ) videoFirstQuartileViews,
    SAFE_CAST( report.attributed_sales_new_to_brand_14_d AS STRING ) attributedSalesNewToBrand14d,
    SAFE_CAST( report.attributed_units_ordered_14_d AS STRING ) attributedUnitsOrdered14d,
    SAFE_CAST( report.attributed_conversions_1_d AS STRING ) attributedConversions1d,
    SAFE_CAST( report.currency AS STRING ) currency,
    SAFE_CAST( report.targeting_type AS STRING ) targetingType,
    SAFE_CAST( report.attributed_sales_30_d AS STRING ) attributedSales30d,
    SAFE_CAST( report.attributed_sales_7_d_same_sku AS STRING ) attributedSales7dSameSku,
    SAFE_CAST( report.impressions AS STRING ) impressions,
    SAFE_CAST( adgroup.name AS STRING ) adGroupName,
    SAFE_CAST( report.cost AS STRING ) cost,
    SAFE_CAST( report.attributed_sales_7_d AS STRING ) attributedSales7d,
    SAFE_CAST( report.report_date AS DATE ) date,
    SAFE_CAST( report.attributed_conversions_14_d AS STRING ) attributedConversions14d,
    SAFE_CAST( report.attributed_conversions_30_d_same_sku AS STRING ) attributedConversions30dSameSku,
    SAFE_CAST( report.attributed_conversions_30_d AS STRING ) attributedConversions30d,

		from report
		left join campaign
			on campaign.id = report.campaign_id
		left join profile
			on profile.id = SAFE_CAST(campaign.profile_id as INT64)
		left join adgroup
			on report.ad_group_id = report.ad_group_id








)

select * from fields
