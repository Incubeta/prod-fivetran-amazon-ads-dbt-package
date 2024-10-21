{{ config(enabled=var('amazon_ads__brandcampaignproductcollection_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_brandcampaignproductcollection_alias','amazon-ad_performance-sponsored_brandcampaignproductcollection-v1' ),
	partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
  )

}}


with report as (
	select *
	from {{ source('dbt_amazon_ads', 'sb_placement_report') }}
),

campaign as (
	select * from {{ source('dbt_amazon_ads', 'sb_campaign_history')}}
),

profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),

fields as (
	select 
		SAFE_CAST(report.currency as STRING) as currency,
		campaign.name as campaignName,
		SAFE_CAST(report.attributed_sales_14_d as STRING) as attributedSales14d,
		report.report_date as date,
		SAFE_CAST(report.attributed_conversions_14_d as STRING) as attributedConversions14d,
		campaign.id as campaignId,
		campaign.state as campaignStatus,
		SAFE_CAST(report.clicks as STRING) as clicks,
		SAFE_CAST(report.cost as STRING) as cost,
		SAFE_CAST(report.impressions as STRING) as impressions,
		report.placement as placement,
		profile.country_code as profileCountryCode,
		campaign.profile_id as profileId,
		
		SAFE_CAST( report.units_sold_14_d as STRING) as unitsSold14d,

		from report
		left join campaign
			on campaign.id = report.campaign_id
		left join profile
			on campaign.profile_id = profile.id








)

select * from fields
