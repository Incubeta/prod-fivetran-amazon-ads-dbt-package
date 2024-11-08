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
	select id, name, state, profile_id,row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'sd_campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
fields as (
        select
                SAFE_CAST(campaigns.name AS STRING) campaignName,
                SAFE_CAST(report.cost AS STRING) cost,
                SAFE_CAST(report.impressions    AS      STRING) impressions,
                SAFE_CAST(report.clicks AS      STRING) clicks,
                SAFE_CAST(profile.currency_code       AS STRING )     currency ,      --using TRIM to get rid of trailing whitespace
                SAFE_CAST(profile.account_name as STRING) profileBrandName,


                SAFE_CAST(campaigns.id  AS      STRING) campaignId,
                SAFE_CAST(profile.id AS STRING) profileId,
                SAFE_CAST(report.date    AS      DATE)   date,
   
   
                SAFE_CAST(profile.country_code  AS      STRING) profileCountryCode,
                SAFE_CAST(campaigns.state AS    STRING) campaignStatus,
                SAFE_CAST(report.campaign_budget_currency_code AS STRING) campaignBudgetCurrencyCode,
                SAFE_CAST(report.purchases_clicks AS STRING) purchasesClicks,
                SAFE_CAST(report.sales_clicks AS STRING) salesClicks
                SAFE_CAST(null as STRING) attributedSales14d,
                SAFE_CAST(null AS STRING) attributedConversions14d,
                SAFE_CAST(null as STRING) attributedUnitsSold14d,
                SAFE_CAST(null as STRING) attributedUnitsOrdered14d

		from report
		left join campaigns
			on campaigns.id = report.campaign_id and campaigns.is_most_recent_record
		left join profile
			on profile.id = campaigns.profile_id







)

select * from fields
