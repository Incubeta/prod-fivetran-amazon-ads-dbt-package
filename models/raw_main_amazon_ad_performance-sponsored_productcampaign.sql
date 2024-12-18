{{ config(enabled=var('amazon_ads__productcampaign_enabled', True))}}
{{
config(
	alias=var('amazon_ads__sponsored_productcampaign_alias','amazon-ad_performance-sponsored_productcampaign-v1' ),
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
	from {{ source('dbt_amazon_ads', 'campaign_level_report') }}
),

campaign as (
	select id, name, state,profile_id, row_number() over(partition by id order by last_updated_date desc)=1 as is_most_recent_record from {{ source('dbt_amazon_ads', 'campaign_history')}}
),
profile as (
	select * from {{ source('dbt_amazon_ads', 'profile')}}
),
fields as (
       select
                SAFE_CAST(campaign.name AS STRING) campaignName,
                SAFE_CAST(report.impressions    AS      STRING) impressions,
                SAFE_CAST(report.clicks AS      STRING) clicks,
                SAFE_CAST(report.campaign_budget_currency_code AS STRING) campaignBudgetCurrencyCode,
                SAFE_CAST(profile.currency_code       AS STRING )     currency ,      --using TRIM to get rid of trailing whitespace
                SAFE_CAST(report.units_sold_clicks_14_d as STRING) unitsSoldClicks14d,
                SAFE_CAST(campaign.state AS    STRING) campaignStatus,
                SAFE_CAST(profile.id AS STRING) profileId,
                SAFE_CAST(profile.account_name as STRING) profileBrandName,
                SAFE_CAST(campaign.id  AS      STRING) campaignId,
                SAFE_CAST(report.purchases_14_d as STRING) purchases14d,
                SAFE_CAST(report.date    AS      DATE)   date,
                SAFE_CAST(report.cost AS STRING) cost,
                SAFE_CAST(report.sales_14_d as STRING) sales14d,
                SAFE_CAST(profile.country_code  AS      STRING) profileCountryCode
		from report
		left join campaign
			on SAFE_CAST(campaign.id as INT64) = report.campaign_id and campaign.is_most_recent_record
		left join profile
			on profile.id = SAFE_CAST(campaign.profile_id as INT64)








)

select * from fields
