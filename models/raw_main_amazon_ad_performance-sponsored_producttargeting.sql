{{ config(enabled=var('amazon_ads__producttargeting_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_producttargeting_alias','amazon-ad_performance-sponsored_producttargeting-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )

}}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'targeting_report') }}
	union all
	select * from {{ source('dbt_amazon_ads', 'targeting_keyword_report')}}
),
campaign as (
	select id,profile_id,name,row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
adgroup as (
	select id,name, row_number() over(partition by id order by last_updated_ddate desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'ad_group_history')}}
),
fields as (
	select 
    SAFE_CAST( report.targeting AS STRING ) targeting,
    SAFE_CAST( report.units_sold_clicks_7_d AS STRING ) unitsSoldClicks7d,
    SAFE_CAST( report.units_sold_same_sku_1_d AS STRING ) unitsSoldSameSku1d,
    SAFE_CAST( report.sales_7_d AS STRING ) sales7d,
    SAFE_CAST( report.purchases_same_sku_7_d AS STRING ) purchasesSameSku7d,
    SAFE_CAST( report.attributed_sales_same_sku_30_d AS STRING ) attributedSalesSameSku30d,
    SAFE_CAST( report.units_sold_other_sku_7_d AS STRING ) unitsSoldOtherSku7d,
    SAFE_CAST( campaign.name AS STRING ) campaignName,
    SAFE_CAST( adgroup.id AS STRING ) adGroupId,
    SAFE_CAST( report.units_sold_same_sku_14_d AS STRING ) unitsSoldSameSku14d,
    SAFE_CAST( report.sales_14_d AS STRING ) sales14d,
    SAFE_CAST( report.units_sold_same_sku_7_d AS STRING ) unitsSoldSameSku7d,
    SAFE_CAST( report.match_type AS STRING ) matchType,
    SAFE_CAST( profile.country_code AS STRING ) profileCountryCode,
    SAFE_CAST( report.keyword_id AS STRING ) keywordId,
    SAFE_CAST( profile.id AS STRING ) profileId,
    SAFE_CAST( report.sales_30_d AS STRING ) sales30d,
    SAFE_CAST( report.clicks AS STRING ) clicks,
    SAFE_CAST( report.purchases_same_sku_14_d AS STRING ) purchasesSameSku14d,
    SAFE_CAST( report.purchases_1_d AS STRING ) purchases1d,
    SAFE_CAST( report.attributed_sales_same_sku_7_d AS STRING ) attributedSalesSameSku7d,
    SAFE_CAST( report.campaign_id AS STRING ) campaignId,
    SAFE_CAST( profile.account_name AS STRING ) profileBrandName,
    SAFE_CAST( report.sales_1_d AS STRING ) sales1d,
    SAFE_CAST( report.campaign_budget_currency_code  AS STRING ) currency,
    SAFE_CAST( report.units_sold_clicks_30_d AS STRING ) unitsSoldClicks30d,
    SAFE_CAST( report.keyword_type AS STRING ) keyword_type,
    SAFE_CAST( report.units_sold_clicks_14_d AS STRING ) unitsSoldClicks14d,
    SAFE_CAST( report.purchases_7_d AS STRING ) purchases7d,
    SAFE_CAST( report.attributed_sales_same_sku_1_d AS STRING ) attributedSalesSameSku1d,
    SAFE_CAST( report.purchases_14_d AS STRING ) purchases14d,
    SAFE_CAST( report.impressions AS STRING ) impressions,
    SAFE_CAST( adgroup.name AS STRING ) adGroupName,
    SAFE_CAST( report.cost AS STRING ) cost,
    SAFE_CAST( report.purchases_same_sku_1_d AS STRING ) purchasesSameSku1d,
    SAFE_CAST( report.purchases_30_d AS STRING ) purchases30d,
    SAFE_CAST( keyword.keyword_text AS STRING ) keyword,
    SAFE_CAST( report.attributed_sales_same_sku_14_d AS STRING ) attributedSalesSameSku14d,
    SAFE_CAST( report.date AS DATE ) date,
    SAFE_CAST( report.units_sold_same_sku_30_d AS STRING ) unitsSoldSameSku30d,
    SAFE_CAST( report.units_sold_clicks_1_d AS STRING ) unitsSoldClicks1d,
    SAFE_CAST( report.purchases_same_sku_30_d AS STRING ) purchasesSameSku30d,

		from report
		left join campaign
			on SAFE_CAST(campaign.id as INT64) = report.campaign_id and campaign.is_most_recent_record
		left join profile
			on profile.id = campaign.profile_id
		left join adgroup
			on report.ad_group_id = SAFE_CAST(adgroup.id as INT64) and adgroup.is_most_recent_record








)

select * from fields
