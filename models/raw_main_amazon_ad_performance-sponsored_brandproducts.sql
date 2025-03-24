{{ config(enabled=var('amazon_ads__brandproducts_enabled', True)) }}
{{
config(
	alias=var('amazon_ads__sponsored_brandproducts_alias','amazon-ad_performance-sponsored_brandproducts-v1' ),
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
    from {{ source('dbt_amazon_ads', 'sb_purchased_product') }}
),

campaigns as (
    select * from {{ source('dbt_amazon_ads', 'sb_campaign_history') }}
),

adgroups as (
    select * from {{ source('dbt_amazon_ads', 'sb_ad_group_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        -- is this the correct brand name?
        profile.account_name as profilebrandname,

        campaigns.name as campaignname,
        report.product_name as productname,
        report.attribution_type as attributiontype,
        adgroups.name as adgroupname,
        report.product_category as productcategory,
        profile.currency_code as profilecurrencycode,
        report.purchased_asin as purchasedasin,
        report.campaign_budget_currency_code as campaignbudgetcurrencycode,
        report.date,
        SAFE_CAST(report.new_to_brand_purchases_14_d as STRING)
            as newtobrandpurchases14d,
        SAFE_CAST(report.new_to_brand_sales_14_d as STRING)
            as newtobrandsales14d,
        SAFE_CAST(report.sales_14_d as STRING) as sales14d,
        SAFE_CAST(campaigns.profile_id as STRING) as profileid,
        SAFE_CAST(profile.country_code as STRING) as profilecountrycode,
        SAFE_CAST(report.units_sold_14_d as STRING) as unitssold14d,
        SAFE_CAST(report.orders_14_d as STRING) as orders14d,
        SAFE_CAST(report.new_to_brand_units_sold_14_d as STRING)
            as newtobrandunitssold14d

    from report
    left join campaigns
        on report.campaign_id = campaigns.id
    left join adgroups
        on report.ad_group_id = adgroups.id
    left join profile
        on campaigns.profile_id = profile.id








)

select * from fields
