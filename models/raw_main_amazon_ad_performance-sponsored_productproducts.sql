{{ config(enabled=var('amazon_ads__productproducts_enabled', True),event_time="date") }}
{{
config(
	alias=var('amazon_ads__sponsored_productproducts_alias','amazon-ad_performance-sponsored_productproducts-v1' ),
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
    from {{ source('dbt_amazon_ads', 'advertised_product_report') }}
),

campaign as (
    select
        id,
        name,
        profile_id,
        targeting_type,
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

fields as (
    select
        'sponsoredProducts' as campaigntype,
        safe_cast(report.units_sold_clicks_7_d as STRING) as unitssoldclicks7d,
        safe_cast(report.units_sold_same_sku_1_d as STRING)
            as unitssoldsamesku1d,
        safe_cast(report.sales_7_d as STRING) as sales7d,
        safe_cast(report.purchases_same_sku_7_d as STRING)
            as purchasessamesku7d,
        safe_cast(report.attributed_sales_same_sku_30_d as STRING)
            as attributedsalessamesku30d,
        safe_cast(report.spend as STRING) as spend,
        safe_cast(report.units_sold_other_sku_7_d as STRING)
            as unitssoldothersku7d,
        safe_cast(campaign.name as STRING) as campaignname,
        safe_cast(report.ad_id as STRING) as adid,
        safe_cast(report.ad_group_id as STRING) as adgroupid,
        safe_cast(report.units_sold_same_sku_14_d as STRING)
            as unitssoldsamesku14d,
        safe_cast(report.sales_14_d as STRING) as sales14d,
        safe_cast(report.units_sold_same_sku_7_d as STRING)
            as unitssoldsamesku7d,
        safe_cast(profile.country_code as STRING) as profilecountrycode,
        safe_cast(report.advertised_asin as STRING) as advertisedasin,
        safe_cast(profile.id as STRING) as profileid,
        safe_cast(report.sales_30_d as STRING) as sales30d,
        safe_cast(report.clicks as STRING) as clicks,
        safe_cast(report.purchases_same_sku_14_d as STRING)
            as purchasessamesku14d,
        safe_cast(report.purchases_same_sku_30_d as STRING)
            as purchasessamesku30d,
        safe_cast(report.purchases_1_d as STRING) as purchases1d,
        safe_cast(report.attributed_sales_same_sku_7_d as STRING)
            as attributedsalessamesku7d,
        safe_cast(campaign.id as STRING) as campaignid,
        safe_cast(profile.account_name as STRING) as profilebrandname,
        safe_cast(report.sales_1_d as STRING) as sales1d,
        safe_cast(report.campaign_budget_currency_code as STRING) as currency,

        safe_cast(report.units_sold_clicks_30_d as STRING)
            as unitssoldclicks30d,
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
        safe_cast(report.attributed_sales_same_sku_14_d as STRING)
            as attributedsalessamesku14d,
        safe_cast(report.date as DATE) as date,
        safe_cast(report.units_sold_same_sku_30_d as STRING)
            as unitssoldsamesku30d,
        safe_cast(report.units_sold_clicks_1_d as STRING) as unitssoldclicks1d,
        safe_cast(report.sales_other_sku_7_d as STRING) as salesothersku7d, -- seems to be always the same value in adverity, but is not found in fivetran
        safe_cast(campaign.targeting_type as STRING) as keywordtype

    from report
    left join campaign
        on
            safe_cast(campaign.id as INT64) = report.campaign_id
            and campaign.is_most_recent_record
    left join profile
        on profile.id = safe_cast(campaign.profile_id as INT64)
    left join adgroup
        on
            report.ad_group_id = safe_cast(adgroup.id as INT64)
            and adgroup.is_most_recent_record








)

select * from fields
