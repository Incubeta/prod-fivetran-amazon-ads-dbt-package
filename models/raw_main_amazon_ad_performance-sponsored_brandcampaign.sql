{{ config(enabled=var('amazon_ads__brandcampaigns_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_brandcampaign_alias','amazon-ad_performance-sponsored_brandcampaign-v1' ),
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
	from {{ source('dbt_amazon_ads', 'sb_campaign_report') }}
),

campaigns as (
	select id,name,state,profile_id, row_number() over(partition by id order by last_update_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'sb_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
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
		profile.account_name as profileBrandName, -- is this the correct brand name?
		report.report_date as date,
		SAFE_CAST(report.cost as STRING) as cost,
		SAFE_CAST(profile.country_code as STRING) as profileCountryCode


		from report
		left join campaigns
			on campaigns.id = report.campaign_id and campaigns.is_most_recent_record
		left join profile
			on campaigns.profile_id = profile.id








)

select * from fields
