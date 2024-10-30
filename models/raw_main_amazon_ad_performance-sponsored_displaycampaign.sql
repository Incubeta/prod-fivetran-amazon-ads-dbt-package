{{ config(enabled=var('amazon_ads__displaycampaign_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_displaycampaign_alias','amazon-ad_performance-sponsored_displaycampaign-v1' ),
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
	from {{ source('dbt_amazon_ads', 'sd_campaign_report') }}
),

campaigns as (
	select * from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
fields as (
	select 
		SAFE_CAST(report.currency	AS STRING )	currency ,	--using TRIM to get rid of trailing whitespace		
		SAFE_CAST(campaigns.name AS STRING) campaignName,
		SAFE_CAST(report.attributed_sales_14_d	AS	STRING)	attributedSales14d,			
		SAFE_CAST(report.report_date	AS	DATE)	date,
		SAFE_CAST(report.attributed_conversions_14_d	AS	STRING) attributedConversions14d,
		SAFE_CAST(campaigns.id	AS	STRING)	campaignId,
		SAFE_CAST(campaigns.state AS	STRING)	campaignStatus,
		SAFE_CAST(report.clicks	AS	STRING)	clicks,
		SAFE_CAST(report.cost AS STRING) cost,
		SAFE_CAST(report.impressions	AS	STRING)	impressions,
		SAFE_CAST(profile.country_code	AS	STRING)	profileCountryCode,
		SAFE_CAST(profile.id AS STRING) profileId,

		from report
		left join campaigns
			on campaigns.id = report.campaign_id
		left join profile
			on profile.id = campaigns.profile_id







)

select * from fields
