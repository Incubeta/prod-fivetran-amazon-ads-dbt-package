{{ config(enabled=var('amazon_ads__brandproducts_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_brandproducts_alias','amazon-ad_performance-sponsored_brandproducts-v1' ),
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
	from {{ source('dbt_amazon_ads', 'sb_purchased_product') }}
),

campaigns as (
	select * from {{ source('dbt_amazon_ads', 'sb_campaign_history')}}
),
adgroups as (
	select * from {{ source('dbt_amazon_ads', 'sb_ad_group_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
fields as (
	select 
		profile.account_name as profileBrandName, -- is this the correct brand name?

		campaigns.name as campaignName,
		report.product_name as productName,
		report.attribution_type as attributionType,
		SAFE_CAST(report.new_to_brand_purchases_14_d as STRING) as newToBrandPurchases14d,
		adgroups.name as adGroupName,
		SAFE_CAST(report.new_to_brand_sales_14_d as STRING) as newToBrandSales14d,
		report.product_category as productCategory,
		SAFE_CAST(report.sales_14_d as STRING) as sales14d,
		profile.currency_code as profileCurrencyCode,
		report.purchased_asin as purchasedAsin,
		report.campaign_budget_currency_code as campaignBudgetCurrencyCode,
		campaigns.profile_id as profileId,
		SAFE_CAST(profile.country_code as STRING) as profileCountryCode,
		SAFE_CAST(report.units_sold_14_d as STRING) unitsSold14d,
		SAFE_CAST(report.orders_14_d as STRING) orders14d,
		SAFE_CAST(report.new_to_brand_units_sold_14_d as STRING) newToBrandUnitsSold14d,
		report.date as date,

		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join adgroups
			on adgroups.id = report.ad_group_id
		left join profile
			on profile.id = campaigns.profile_id








)

select * from fields
