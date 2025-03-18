{{ config(enabled=var('amazon_ads__productcampaign_enabled', True)) }}
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
    select
        id,
        name,
        state,
        profile_id,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        safe_cast(campaign.name as STRING) as campaignname,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.campaign_budget_currency_code as STRING)
            as campaignbudgetcurrencycode,
        --using TRIM to get rid of trailing whitespace
        safe_cast(profile.currency_code as STRING) as currency,
        safe_cast(report.units_sold_clicks_14_d as STRING)
            as unitssoldclicks14d,
        safe_cast(campaign.state as STRING) as campaignstatus,
        safe_cast(profile.id as STRING) as profileid,
        safe_cast(profile.account_name as STRING) as profilebrandname,
        safe_cast(campaign.id as STRING) as campaignid,
        safe_cast(report.purchases_14_d as STRING) as purchases14d,
        safe_cast(report.date as DATE) as date,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(report.sales_14_d as STRING) as sales14d,
        safe_cast(profile.country_code as STRING) as profilecountrycode
    from report
    left join campaign
        on
            safe_cast(campaign.id as INT64) = report.campaign_id
            and campaign.is_most_recent_record
    left join profile
        on profile.id = safe_cast(campaign.profile_id as INT64)








)

select * from fields
