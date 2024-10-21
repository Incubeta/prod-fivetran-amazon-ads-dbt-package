{{ config(enabled=var('amazon_ads__brandcampaigns_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_brandcampaign_alias','amazon-ad_performance-sponsored_brandcampaign-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )

}}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'sb_campaign_report') }}
),

campaigns as (
	select * from {{ source('dbt_amazon_ads', 'sb_campaign_history')}}
),
ads as (
	select * from {{ source('dbt_amazon_ads','sb_ad_history')}}
),
creative as (
	select * from {{ source('dbt_amazon_ads','sb_creative_history')}}
),


fields as (
	select 
		campaigns.name as campaignName,
		SAFE_CAST( report.units_sold_14_d as STRING) as unitsSold14d,
		SAFE_CAST(report.impressions as STRING) as impressions,
		SAFE_CAST(report.clicks as STRING) as clicks,
		SAFE_CAST(report.currency as STRING) as currency,
		campaigns.state as campaignStatus,
		SAFE_CAST(report.attributed_sales_14_d as STRING) as attributedSales14d,
		SAFE_CAST(report.attributed_conversions_14_d as STRING) as attributedConversions14d,
		campaigns.id as campaignId,
		campaigns.profile_id as profileId,
		creative.brand_name as profileBrandName, -- is this the correct brand name?
		report.report_date as date,
		SAFE_CAST(report.cost as STRING) as cost

		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join ads
			on ads.campaign_id = campaigns.id
		left join creative
			on creative.ad_id = ads.id








)

select * from fields
