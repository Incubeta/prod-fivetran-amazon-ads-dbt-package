{{ config(enabled=var('amazon_ads__displayasin_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_displayasin_alias','amazon-ad_performance-sponsored_displayasin-v1' ),
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
	from {{ source('dbt_amazon_ads', 'sd_asin_report') }}
),

campaigns as (
	select * from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
adgroup as (
	select * from {{ source('dbt_amazon_ads', 'sd_ad_group_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
fields as (
	select 
	    SAFE_CAST( report.currency AS STRING ) currency,
	    SAFE_CAST( campaigns.name AS STRING ) campaignName,
	    report.report_date as date,
	    SAFE_CAST( report.campaign_id AS STRING ) campaignId,
	    SAFE_CAST( profile.country_code AS STRING ) profileCountryCode,
	    SAFE_CAST( profile.id AS STRING ) profileId,
	    SAFE_CAST( report.attributed_units_ordered_30_d_other_sku AS STRING ) attributedUnitsOrdered30dOtherSKU,
	    SAFE_CAST( report.attributed_units_ordered_14_d_other_sku AS STRING ) attributedUnitsOrdered14dOtherSKU,
	    SAFE_CAST( report.attributed_units_ordered_1_d_other_sku AS STRING ) attributedUnitsOrdered1dOtherSKU,
	    SAFE_CAST( report.attributed_units_ordered_7_d_other_sku AS STRING ) attributedUnitsOrdered7dOtherSKU,
	    SAFE_CAST( adgroup.name as STRING) adgroupName,
	    SAFE_CAST( report.sku AS STRING ) sku,
	    SAFE_CAST( report.ad_group_id as STRING) adGroupId,
	    SAFE_CAST( profile.account_name as STRING) profileBrandName,
	SAFE_CAST(report.asin as STRING) asin,
	SAFE_CAST(report.other_asin as STRING) otherAsin,
	SAFE_CAST(report.attributed_sales_7_d_other_sku as STRING) attributedSales7dOtherSku,
	SAFE_CAST(report.attributed_sales_14_d_other_sku as STRING) attributedSales14dOtherSku,
	SAFE_CAST(report.attributed_sales_30_d_other_sku as STRING) attributedSales30dOtherSku,
	SAFE_CAST(report.attributed_sales_1_d_other_sku as STRING) attributedSales1dOtherSku,


		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join profile
			on profile.id = campaigns.profile_id
		left join adgroup
			on report.ad_group_id = adgroup.id








)

select * from fields
