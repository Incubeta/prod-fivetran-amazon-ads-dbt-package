{{ config(enabled=var('amazon_ads__displayproducts_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_displayproducts_alias','amazon-ad_performance-sponsored_displayproducts-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
	materialized='incremental',
	incremental_strategy='insert_overwrite'
  )

}}

{%- set report_columns = dbt_utils.get_filtered_columns_in_relation(from=source('dbt_amazon_ads', 'sd_product_ad_report')) -%}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'sd_product_ad_report') }}
),

campaigns as (
	select id,name,profile_id, row_number() over (partition by id order by last_updated_date desc) = 1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
product_ad_history as (
	select id, asin, campaign_id, ad_group_id, row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record  from {{ source('dbt_amazon_ads', 'sd_product_ad_history')}}
),
fields as (
	select 
	{% if 'report_date' in report_columns%}
	COALESCE(report.report_date, report.date) as date,
	{% else %}
	report.date as date,
	{% endif %}
    SAFE_CAST( report.campaign_id AS STRING ) campaignId,
    SAFE_CAST( campaigns.name AS STRING ) campaignName,
    SAFE_CAST( report.ad_id AS STRING ) adId,
    SAFE_CAST( profile.country_code AS STRING ) profileCountryCode,
    SAFE_CAST( profile.id AS STRING ) profileId,
    SAFE_CAST( profile.account_name AS STRING ) profileBrandName,
    SAFE_CAST( profile.currency_code AS STRING ) campaignBudgetCurrencyCode,

    SAFE_CAST( report.currency AS STRING ) currency,
    SAFE_CAST( product_ad_history.asin AS STRING ) productAdsAsin,
    SAFE_CAST( report.attributed_sales_1_d_same_sku AS STRING ) attributedSales1dSameSKU,
    SAFE_CAST( report.attributed_units_ordered_7_d AS STRING ) attributedUnitsOrdered7d,
    SAFE_CAST( report.attributed_conversions_7_d_same_sku AS STRING ) attributedConversions7dSameSKU,
    SAFE_CAST( report.video_complete_views AS STRING ) videoCompleteViews,
    SAFE_CAST( report.attributed_orders_new_to_brand_14_d AS STRING ) attributedOrdersNewToBrand14d,
    SAFE_CAST( report.video_unmutes AS STRING ) videoUnmutes,
    SAFE_CAST( report.attributed_conversions_14_d_same_sku AS STRING ) attributedConversions14dSameSKU,
    SAFE_CAST( report.video_third_quartile_views AS STRING ) videoThirdQuartileViews,
    SAFE_CAST( report.attributed_sales_30_d_same_sku AS STRING ) attributedSales30dSameSKU,
    SAFE_CAST( report.attributed_conversions_1_d_same_sku AS STRING ) attributedConversions1dSameSKU,
    SAFE_CAST( report.attributed_sales_1_d AS STRING ) attributedSales1d,
    SAFE_CAST( report.attributed_sales_14_d AS STRING ) attributedSales14d,
    SAFE_CAST( report.attributed_sales_14_d_same_sku AS STRING ) attributedSales14dSameSku,
    SAFE_CAST( report.attributed_conversions_7_d AS STRING ) attributedConversions7d,
    SAFE_CAST( report.attributed_units_ordered_1_d AS STRING ) attributedUnitsOrdered1d,
    SAFE_CAST( report.attributed_units_ordered_30_d AS STRING ) attributedUnitsOrdered30d,
    SAFE_CAST( report.video_midpoint_views AS STRING ) videoMidpointViews,
    SAFE_CAST( report.attributed_units_ordered_new_to_brand_14_d AS STRING ) attributedUnitsOrderedNewToBrand14d,
    SAFE_CAST( report.view_impressions AS STRING ) viewImpressions,
    SAFE_CAST( report.video_first_quartile_views AS STRING ) videoFirstQuartileViews,
    SAFE_CAST( report.attributed_sales_new_to_brand_14_d AS STRING ) attributedSalesNewToBrand14d,
    SAFE_CAST( report.attributed_units_ordered_14_d AS STRING ) attributedUnitsOrdered14d,
    SAFE_CAST( report.attributed_conversions_1_d AS STRING ) attributedConversions1d,
    SAFE_CAST( report.attributed_sales_30_d AS STRING ) attributedSales30d,
    SAFE_CAST( report.attributed_sales_7_d_same_sku AS STRING ) attributedSales7dSameSKU,
    SAFE_CAST( report.impressions AS STRING ) impressions,
    SAFE_CAST( report.clicks AS STRING ) clicks,
    SAFE_CAST( report.cost AS STRING ) cost,
    SAFE_CAST( report.attributed_sales_7_d AS STRING ) attributedSales7d,
    SAFE_CAST( report.attributed_conversions_14_d AS STRING ) attributedConversions14d,
    SAFE_CAST( report.attributed_conversions_30_d_same_sku AS STRING ) attributedConversions30dSameSKU,
    SAFE_CAST( report.attributed_conversions_30_d AS STRING ) attributedConversions30d,
    SAFE_CAST( report.sales_clicks AS STRING ) salesClicks,
    SAFE_CAST( report.new_to_brand_sales_clicks AS STRING ) newToBrandSalesClicks,
    SAFE_CAST( report.sales_promoted_clicks AS STRING ) salesPromotedClicks,
    SAFE_CAST( report.new_to_brand_purchases_clicks AS STRING ) newToBrandPurchasesClicks,
    SAFE_CAST( report.new_to_brand_units_sold_clicks AS STRING ) newToBrandUnitsSoldClicks,
    SAFE_CAST( report.purchases_promoted_clicks AS STRING ) purchasesPromotedClicks,
    SAFE_CAST( report.impressions_views AS STRING ) impressionsViews,
    SAFE_CAST( report.purchases_clicks AS STRING ) purchasesClicks,


		from report
		left join campaigns
			on campaigns.id = report.campaign_id and campaigns.is_most_recent_record
		left join profile
			on profile.id = campaigns.profile_id
		left join product_ad_history
			on product_ad_history.campaign_id = report.campaign_id and product_ad_history.ad_group_id = report.ad_group_id and product_ad_history.is_most_recent_record and product_ad_history.id = report.ad_id








)

select * from fields
