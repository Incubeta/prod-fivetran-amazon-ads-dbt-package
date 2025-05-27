{{ config(enabled=var('amazon_ads__brandtargeting_enabled', True),event_time="date") }}
{{
config(
	alias=var('amazon_ads__sponsored_brandtargeting_alias','amazon-ad_performance-sponsored_brandtargeting-v1' ),
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
    from {{ source('dbt_amazon_ads', 'sb_keyword_report') }}
),

keyword as (
    select *
    from {{ source('dbt_amazon_ads', 'sb_keyword') }}
),

campaigns as (
    select
        id,
        name,
        profile_id,
        serving_status,
        cost_type,
        budget_type,
        row_number() over (
            partition by id
            order by last_update_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sb_campaign_history') }}
),

adgroups as (
    select
        id,
        name,
        row_number() over (
            partition by id
            order by last_update_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'sb_ad_group_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        campaigns.name as campaignname,
        report.report_date as date,
        report.campaign_id as campaignid,
        safe_cast(report.ad_group_id as STRING) as adgroupid,
        safe_cast(report.attributed_conversions_14_d as STRING)
            as attributedconversions14d,
        safe_cast(report.attributed_sales_14_d as STRING) as attributedsales14d,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(report.currency as STRING) as currency,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(profile.country_code as STRING) as profilecountrycode,
        safe_cast(profile.account_name as STRING) as profilebrandname,
        safe_cast(adgroups.name as STRING) as adgroupname,
        safe_cast(keyword.keyword_text as STRING) as keywordtext,
        safe_cast(keyword.match_type as STRING) as matchtype,
        safe_cast(report.attributed_conversions_14_d_same_sku as STRING)
            as attributedconversions14dsamesku,
        safe_cast(report.attributed_sales_14_d_same_sku as STRING)
            as attributedsales14dsamesku,
        safe_cast(campaigns.serving_status as STRING) as campaignstatus,
        safe_cast(campaigns.cost_type as STRING) as campaigntype,
        safe_cast(report.search_term_impression_rank as STRING)
            as searchtermimpressionrank,
        safe_cast(report.top_of_search_impression_share as STRING)
            as topofsearchimpressionshare,
        safe_cast(report.search_term_impression_share as STRING)
            as searchtermimpressionshare,
        safe_cast(campaigns.budget_type as STRING) as campaignbudgettype,
        safe_cast(report.units_sold_14_d as STRING) as unitssold14d


    from report
    left join campaigns
        on report.campaign_id = campaigns.id and campaigns.is_most_recent_record
    left join adgroups
        on report.ad_group_id = adgroups.id and adgroups.is_most_recent_record
    left join profile
        on campaigns.profile_id = profile.id
    left join keyword
        on report.keyword_id = keyword.id








)

select * from fields
