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

    SAFE_CAST( product_ad_history.asin AS STRING ) productAdsAsin,
    SAFE_CAST( report.video_complete_views AS STRING ) videoCompleteViews,
    SAFE_CAST( report.video_unmutes AS STRING ) videoUnmutes,
    SAFE_CAST( report.video_third_quartile_views AS STRING ) videoThirdQuartileViews,
    SAFE_CAST( report.video_midpoint_views AS STRING ) videoMidpointViews,
    SAFE_CAST( report.video_first_quartile_views AS STRING ) videoFirstQuartileViews,
    SAFE_CAST( report.impressions AS STRING ) impressions,
    SAFE_CAST( report.clicks AS STRING ) clicks,
    SAFE_CAST( report.cost AS STRING ) cost,
    SAFE_CAST( report.sales_clicks AS STRING ) salesClicks,
    SAFE_CAST( report.new_to_brand_sales_clicks AS STRING ) newToBrandSalesClicks,
    SAFE_CAST( report.sales_promoted_clicks AS STRING ) salesPromotedClicks,
    SAFE_CAST( report.new_to_brand_purchases_clicks AS STRING ) newToBrandPurchasesClicks,
    SAFE_CAST( report.new_to_brand_units_sold_clicks AS STRING ) newToBrandUnitsSoldClicks,
    SAFE_CAST( report.purchases_promoted_clicks AS STRING ) purchasesPromotedClicks,
    SAFE_CAST( report.impressions_views AS STRING ) impressionsViews,
    SAFE_CAST( report.purchases_clicks AS STRING ) purchasesClicks,
SAFE_CAST(null as STRING) as attributedConversions14d,
SAFE_CAST( null as STRING) as attributedConversions14dSameSKU,
	SAFE_CAST(null as STRING) as attributedConversions1d,
	SAFE_CAST(null as STRING) as attributedConversions1dSameSKU,
	SAFE_CAST(null as STRING) as attributedConversions30d,
	SAFE_CAST(null as STRING) as attributedConversions30dSameSKU,
	SAFE_CAST(null as STRING) as attributedConversions7d,
	SAFE_CAST(null as STRING) as attributedConversions7dSameSKU,
	SAFE_CAST(null as STRING) as attributedOrdersNewToBrand14d,
	SAFE_CAST(null as STRING) as attributedSales14d,
	SAFE_CAST(null as STRING) as attributedSales14dSameSKU,
	SAFE_CAST(null as STRING) as attributedSales1d,
	SAFE_CAST(null as STRING) as attributedSales1dSameSKU,
	SAFE_CAST(null as STRING) as attributedSales30d,
	SAFE_CAST(null as STRING) as attributedSales30dSameSKU,
	SAFE_CAST(null as STRING) as attributedSales7d,
	SAFE_CAST(null as STRING) as attributedSales7dSameSKU,
	SAFE_CAST(null as STRING) as attributedSalesNewToBrand14d,
	SAFE_CAST(null as STRING) as attributedUnitsOrdered1d,
	SAFE_CAST(null as STRING) as attributedUnitsOrdered30d,
	SAFE_CAST(null as STRING) as attributedUnitsOrdered7d,
	SAFE_CAST(null as STRING) as attributedUnitsOrderedNewToBrand14d,
	SAFE_CAST(null as STRING) as currency,
	SAFE_CAST(null as STRING) as viewImpressions




		from report
		left join campaigns
			on campaigns.id = report.campaign_id and campaigns.is_most_recent_record
		left join profile
			on profile.id = campaigns.profile_id
		left join product_ad_history
			on product_ad_history.campaign_id = report.campaign_id and product_ad_history.ad_group_id = report.ad_group_id and product_ad_history.is_most_recent_record and product_ad_history.id = report.ad_id








)

select * from fields
