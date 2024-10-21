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
	from {{ source('dbt_amazon_ads', 'sb_target_report') }}
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
		report.campaign_budget_currency_code as campaignBudgetCurrencyCode,
		report.keyword_text as keywordText,
		report.campaign_status as campaignStatus,
		SAFE_CAST(report.attributed_conversions_14_d_same_sku as STRING) as attributedConversions14dSameSKU,
		report.ad_group_id as adGroupId,
		report.search_term_impression_rank as searchTermImpressionRank,
		SAFE_CAST(report.top_of_search_impression_share as STRING) as topOfSearchImpressionShare, 
		report.campaign_type as campaignType,
		report.search_term_impression_share as search_term_impression_share,
		SAFE_CAST(report.impressions as STRING) as impressions,
		adgroups.name as adGroupName,
		SAFE_CAST(report.cost as STRING) as cost,
		profile.country_code as profileCountryCode,
		report.match_type as matchType,
		SAFE_CAST(report.attributed_sales_14_d_same_sku as STRING) attributedSales14dSameSKU,
		SAFE_CAST(report.attributed_sales_14_d as STRING) attributedSales14d,
		SAFE_CAST(report.clicks as STRING) as clicks,
		SAFE_CAST(report.units_sold_14_d as STRING) unitsSold14d,
		report.campaign_budget_type as campaignBudgetType,
		report.date as date,
		SAFE_CAST(report.attributed_conversions_14_d as STRING) as attributedConversions14d,
		report.campaign_id as campaignId,
		profile.account_name as profileBrandName, -- is this the correct brand name?


		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join adgroups
			on adgroups.id = report.ad_group_id
		left join profile
			on profile.id = campaigns.profile_id








)

select * from fields
