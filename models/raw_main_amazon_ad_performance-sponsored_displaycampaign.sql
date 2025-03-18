{{ config(enabled=var('amazon_ads__displaycampaign_enabled', True)) }}
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
    select
        id,
        name,
        state,
        profile_id,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sd_campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        safe_cast(campaigns.name as STRING) as campaignname,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(report.clicks as STRING) as clicks,
        --using TRIM to get rid of trailing whitespace
        safe_cast(profile.currency_code as STRING) as currency,
        safe_cast(profile.account_name as STRING) as profilebrandname,


        safe_cast(campaigns.id as STRING) as campaignid,
        safe_cast(profile.id as STRING) as profileid,
        safe_cast(report.date as DATE) as date,


        safe_cast(profile.country_code as STRING) as profilecountrycode,
        safe_cast(campaigns.state as STRING) as campaignstatus,
        safe_cast(report.campaign_budget_currency_code as STRING)
            as campaignbudgetcurrencycode,
        safe_cast(report.purchases_clicks as STRING) as purchasesclicks,
        safe_cast(report.sales_clicks as STRING) as salesclicks,
        safe_cast(null as STRING) as attributedsales14d,
        safe_cast(null as STRING) as attributedconversions14d,
        safe_cast(null as STRING) as attributedunitssold14d,
        safe_cast(null as STRING) as attributedunitsordered14d

    from report
    left join campaigns
        on report.campaign_id = campaigns.id and campaigns.is_most_recent_record
    left join profile
        on campaigns.profile_id = profile.id







)

select * from fields
