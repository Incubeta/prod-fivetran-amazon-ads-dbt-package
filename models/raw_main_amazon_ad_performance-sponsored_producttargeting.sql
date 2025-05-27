{{ config(enabled=var('amazon_ads__producttargeting_enabled', True),event_time="date") }}
{{
config(
	alias=var('amazon_ads__sponsored_producttargeting_alias','amazon-ad_performance-sponsored_producttargeting-v1' ),
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
    from {{ source('dbt_amazon_ads', 'targeting_report') }}
    union all
    select * from {{ source('dbt_amazon_ads', 'targeting_keyword_report') }}
),

campaign as (
    select
        id,
        profile_id,
        name,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'campaign_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

adgroup as (
    select
        id,
        name,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'ad_group_history') }}
),

keyword_history as (
    select
        id,
        keyword_text,
        row_number() over (
            partition by id
            order by last_updated_date desc
        ) = 1 as is_most_recent_record
    from {{ source('dbt_amazon_ads', 'keyword_history') }}
),

fields as (
    select
        safe_cast(report.targeting as STRING) as targeting,
        safe_cast(report.units_sold_clicks_7_d as STRING) as unitssoldclicks7d,
        safe_cast(report.units_sold_same_sku_1_d as STRING)
            as unitssoldsamesku1d,
        safe_cast(report.sales_7_d as STRING) as sales7d,
        safe_cast(report.purchases_same_sku_7_d as STRING)
            as purchasessamesku7d,
        safe_cast(report.attributed_sales_same_sku_30_d as STRING)
            as attributedsalessamesku30d,
        safe_cast(report.units_sold_other_sku_7_d as STRING)
            as unitssoldothersku7d,
        safe_cast(campaign.name as STRING) as campaignname,
        safe_cast(adgroup.id as STRING) as adgroupid,
        safe_cast(report.units_sold_same_sku_14_d as STRING)
            as unitssoldsamesku14d,
        safe_cast(report.sales_14_d as STRING) as sales14d,
        safe_cast(report.units_sold_same_sku_7_d as STRING)
            as unitssoldsamesku7d,
        safe_cast(report.match_type as STRING) as matchtype,
        safe_cast(profile.country_code as STRING) as profilecountrycode,
        safe_cast(report.keyword_id as STRING) as keywordid,
        safe_cast(profile.id as STRING) as profileid,
        safe_cast(report.sales_30_d as STRING) as sales30d,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.purchases_same_sku_14_d as STRING)
            as purchasessamesku14d,
        safe_cast(report.purchases_1_d as STRING) as purchases1d,
        safe_cast(report.attributed_sales_same_sku_7_d as STRING)
            as attributedsalessamesku7d,
        safe_cast(report.campaign_id as STRING) as campaignid,
        safe_cast(profile.account_name as STRING) as profilebrandname,
        safe_cast(report.sales_1_d as STRING) as sales1d,
        safe_cast(report.campaign_budget_currency_code as STRING) as currency,
        safe_cast(report.units_sold_clicks_30_d as STRING)
            as unitssoldclicks30d,
        safe_cast(report.keyword_type as STRING) as keywordtype,
        safe_cast(report.units_sold_clicks_14_d as STRING)
            as unitssoldclicks14d,
        safe_cast(report.purchases_7_d as STRING) as purchases7d,
        safe_cast(report.attributed_sales_same_sku_1_d as STRING)
            as attributedsalessamesku1d,
        safe_cast(report.purchases_14_d as STRING) as purchases14d,
        safe_cast(report.impressions as STRING) as impressions,
        safe_cast(adgroup.name as STRING) as adgroupname,
        safe_cast(report.cost as STRING) as cost,
        safe_cast(report.purchases_same_sku_1_d as STRING)
            as purchasessamesku1d,
        safe_cast(report.purchases_30_d as STRING) as purchases30d,
        safe_cast(keyword_history.keyword_text as STRING) as keyword,
        safe_cast(report.attributed_sales_same_sku_14_d as STRING)
            as attributedsalessamesku14d,
        safe_cast(report.date as DATE) as date,
        safe_cast(report.units_sold_same_sku_30_d as STRING)
            as unitssoldsamesku30d,
        safe_cast(report.units_sold_clicks_1_d as STRING) as unitssoldclicks1d,
        safe_cast(report.purchases_same_sku_30_d as STRING)
            as purchasessamesku30d

    from report
    left join campaign
        on
            safe_cast(campaign.id as INT64) = report.campaign_id
            and campaign.is_most_recent_record
    left join profile
        on campaign.profile_id = profile.id
    left join adgroup
        on
            report.ad_group_id = safe_cast(adgroup.id as INT64)
            and adgroup.is_most_recent_record
    left join keyword_history
        on
            safe_cast(keyword_history.id as INT64) = report.keyword_id
            and keyword_history.is_most_recent_record








)

select * from fields
