{{ config(enabled=var('amazon_ads__displayasin_enabled', True)) }}
{{
config(
	alias=var('amazon_ads__sponsored_displayasin_alias','amazon-ad_performance-sponsored_displayasin-v1' ),
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
    from {{ source('dbt_amazon_ads', 'sd_asin_report') }}
),

campaigns as (
    select * from {{ source('dbt_amazon_ads', 'sd_campaign_history') }}
),

adgroup as (
    select * from {{ source('dbt_amazon_ads', 'sd_ad_group_history') }}
),

profile as (
    select * from {{ source('dbt_amazon_ads', 'profile') }}
),

fields as (
    select
        report.report_date as date,
        SAFE_CAST(report.currency as STRING) as currency,
        SAFE_CAST(campaigns.name as STRING) as campaignname,
        SAFE_CAST(report.campaign_id as STRING) as campaignid,
        SAFE_CAST(profile.country_code as STRING) as profilecountrycode,
        SAFE_CAST(profile.id as STRING) as profileid,
        SAFE_CAST(report.attributed_units_ordered_30_d_other_sku as STRING)
            as attributedunitsordered30dothersku,
        SAFE_CAST(report.attributed_units_ordered_14_d_other_sku as STRING)
            as attributedunitsordered14dothersku,
        SAFE_CAST(report.attributed_units_ordered_1_d_other_sku as STRING)
            as attributedunitsordered1dothersku,
        SAFE_CAST(report.attributed_units_ordered_7_d_other_sku as STRING)
            as attributedunitsordered7dothersku,
        SAFE_CAST(adgroup.name as STRING) as adgroupname,
        SAFE_CAST(report.sku as STRING) as sku,
        SAFE_CAST(report.ad_group_id as STRING) as adgroupid,
        SAFE_CAST(profile.account_name as STRING) as profilebrandname,
        SAFE_CAST(report.asin as STRING) as asin,
        SAFE_CAST(report.other_asin as STRING) as otherasin,
        SAFE_CAST(report.attributed_sales_7_d_other_sku as STRING)
            as attributedsales7dothersku,
        SAFE_CAST(report.attributed_sales_14_d_other_sku as STRING)
            as attributedsales14dothersku,
        SAFE_CAST(report.attributed_sales_30_d_other_sku as STRING)
            as attributedsales30dothersku,
        SAFE_CAST(report.attributed_sales_1_d_other_sku as STRING)
            as attributedsales1dothersku


    from report
    left join campaigns
        on report.campaign_id = campaigns.id
    left join profile
        on campaigns.profile_id = profile.id
    left join adgroup
        on report.ad_group_id = adgroup.id








)

select * from fields
