{{ config(enabled=var('amazon_ads__brandtargeting_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_brandtargeting_alias','amazon-ad_performance-sponsored_brandtargeting-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )

}}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'sb_keyword_report') }}
),
with keyword as (
	select * 
	from {{ source('dbt_amazon_ads', 'sb_keyword')}}
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
		campaigns.name as campaignName
		report.date as date,
		SAFE_CAST(report.ad_group_id as STRING) as adGroupId,
	        SAFE_CAST(report.attributed_conversions_14_d as STRING) as attributedConversions14d,
		SAFE_CAST(report.attributed_sales_14_d as STRING) as attributedSales14d,
		report.campaing_id as campaignId
		SAFE_CAST(report.clicks as STRING) as clicks,
		SAFE_CAST(report.cost as STRING) as cost,
		SAFE_CAST(report.currency as STRING) as currency,
		SAFE_CAST(report.impressions as STRING) as impressions,
		SAFE_CAST(profile.country_code as STRING) as profileCountryCode,
		SAFE_CAST(profile.account_name as STRING) as profileBrandName,
		SAFE_CAST(adgroup.name as STRING) as adGroupName,
		SAFE_CAST(keyword.keyword_text as STRING) as keywordText,
		SAFE_CAST(keyword.match_type as STRING) as matchType,
		SAFE_CAST(report.attributed_conversions_14_d_same_sku as STRING) as attributedConversions14dSameSKU,
		SAFE_CAST(report.attributed_sales_14_d_same_sku as STRING) as attributedSales14dSameSKU,
		SAFE_CAST(campaigns.serving_status as STRING) as campignStatus,
		SAFE_CAST(campaigns.cost_type as STRING) as campaignType,
		SAFE_CAST(report.search_term_impression_share as STRING) as searchTermImpressionShare,
		SAFE_CAST(report.top_of_search_impression_share as STRING) as topOfSearchImpressionShare,
		SAFE_CAST(report.search_term_impression_share as STRING) as searchTermImpressionShare,
		SAFE_CAST(campaigns.budget_type as STRING) as budgetType


		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join adgroups
			on adgroups.id = report.ad_group_id
		left join profile
			on profile.id = campaigns.profile_id
		left join keyword
			on report.keyword_id = keyword.id








)

select * from fields
