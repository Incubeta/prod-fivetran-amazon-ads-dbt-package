
{{
  config(
    alias= 'amazon-ad_performance-sponsored_uniontargeting-v1'
  )
}}


WITH amazon_target_union AS (
    SELECT
        SAFE_CAST( day AS DATE ) day,
        SAFE_CAST( profile_brand_name AS STRING ) account,
        SAFE_CAST( profile_country_code AS STRING ) account_country_code,
        SAFE_CAST( currency AS STRING ) currency,
        SAFE_CAST( campaign_name AS STRING ) campaign_name,
        SAFE_CAST( campaign_sub_1 AS STRING ) campaign_sub_1,
        SAFE_CAST( campaign_sub_2 AS STRING ) campaign_sub_2,
        SAFE_CAST( campaign_sub_3 AS STRING ) campaign_sub_3,
        SAFE_CAST( campaign_sub_4 AS STRING ) campaign_sub_4,
        SAFE_CAST( campaign_sub_5 AS STRING ) campaign_sub_5,
        SAFE_CAST( campaign_sub_6 AS STRING ) campaign_sub_6,
        SAFE_CAST( campaign_sub_7 AS STRING ) campaign_sub_7,
        SAFE_CAST( campaign_sub_8 AS STRING ) campaign_sub_8,
        SAFE_CAST( campaign_sub_9 AS STRING ) campaign_sub_9,
        SAFE_CAST( campaign_sub_10 AS STRING ) campaign_sub_10,
        SAFE_CAST( "Sponsored Products" AS STRING ) campaign_type,
        SAFE_CAST( adgroup_name AS STRING ) adgroup_name,
        SAFE_CAST( targeting AS STRING ) targeting,
        SAFE_CAST( match_type AS STRING ) match_type,
        SAFE_CAST( cost AS FLOAT64 ) cost,
        SAFE_CAST( impressions AS INT64 ) impressions,
        SAFE_CAST( clicks AS INT64 ) clicks,
        SAFE_CAST( purchases_7d AS INT64 ) orders,
        SAFE_CAST( units_sold_clicks_7d AS INT64 ) units,
        SAFE_CAST( sales_7d AS FLOAT64 ) revenue,

    FROM

        {{ref('dpl_legacy_amazon-ad_performance-sponsored_producttargeting-v1')}}

    UNION ALL

    SELECT
        SAFE_CAST( day AS DATE ) day,
        SAFE_CAST( profile_brand_name AS STRING ) account,
        SAFE_CAST( profile_country_code AS STRING ) account_country_code,
        SAFE_CAST( currency AS STRING ) currency,
        SAFE_CAST( campaign_name AS STRING ) campaign_name,
        SAFE_CAST( campaign_sub_1 AS STRING ) campaign_sub_1,
        SAFE_CAST( campaign_sub_2 AS STRING ) campaign_sub_2,
        SAFE_CAST( campaign_sub_3 AS STRING ) campaign_sub_3,
        SAFE_CAST( campaign_sub_4 AS STRING ) campaign_sub_4,
        SAFE_CAST( campaign_sub_5 AS STRING ) campaign_sub_5,
        SAFE_CAST( campaign_sub_6 AS STRING ) campaign_sub_6,
        SAFE_CAST( campaign_sub_7 AS STRING ) campaign_sub_7,
        SAFE_CAST( campaign_sub_8 AS STRING ) campaign_sub_8,
        SAFE_CAST( campaign_sub_9 AS STRING ) campaign_sub_9,
        SAFE_CAST( campaign_sub_10 AS STRING ) campaign_sub_10,
        SAFE_CAST( "Sponsored Brands" AS STRING ) campaign_type,
        SAFE_CAST( adgroup_name AS STRING ) adgroup_name,
        SAFE_CAST( keyword_Text AS STRING ) targeting,
        SAFE_CAST( match_type AS STRING ) match_type,
        SAFE_CAST( cost AS FLOAT64 ) cost,
        SAFE_CAST( impressions AS INT64 ) impressions,
        SAFE_CAST( clicks AS INT64 ) clicks,
        SAFE_CAST( attributed_conversions_14d AS INT64 ) orders,
        SAFE_CAST( units_sold_14d AS INT64 ) units,
        SAFE_CAST( attributed_sales_14d AS FLOAT64 ) revenue,

    FROM
        {{ref('dpl_legacy_amazon-ad_performance-sponsored_brandtargeting-v1')}}


    UNION ALL

    SELECT
        SAFE_CAST( day AS DATE ) day,
        SAFE_CAST( profile_brand_name AS STRING ) account,
        SAFE_CAST( profile_country_code AS STRING ) account_country_code,
        SAFE_CAST( currency AS STRING ) currency,
        SAFE_CAST( campaign_name AS STRING ) campaign_name,
        SAFE_CAST( campaign_sub_1 AS STRING ) campaign_sub_1,
        SAFE_CAST( campaign_sub_2 AS STRING ) campaign_sub_2,
        SAFE_CAST( campaign_sub_3 AS STRING ) campaign_sub_3,
        SAFE_CAST( campaign_sub_4 AS STRING ) campaign_sub_4,
        SAFE_CAST( campaign_sub_5 AS STRING ) campaign_sub_5,
        SAFE_CAST( campaign_sub_6 AS STRING ) campaign_sub_6,
        SAFE_CAST( campaign_sub_7 AS STRING ) campaign_sub_7,
        SAFE_CAST( campaign_sub_8 AS STRING ) campaign_sub_8,
        SAFE_CAST( campaign_sub_9 AS STRING ) campaign_sub_9,
        SAFE_CAST( campaign_sub_10 AS STRING ) campaign_sub_10,
        SAFE_CAST( "Sponsored Display" AS STRING ) campaign_type,
        SAFE_CAST( adgroup_name AS STRING ) adgroup_name,
        SAFE_CAST( targeting_Text AS STRING ) targeting,
        SAFE_CAST( targeting_Type AS STRING ) match_type,
        SAFE_CAST( cost AS FLOAT64 ) cost,
        SAFE_CAST( impressions AS INT64 ) impressions,
        SAFE_CAST( clicks AS INT64 ) clicks,
        SAFE_CAST( attributed_conversions_14d AS INT64 ) orders,
        SAFE_CAST( attributed_units_ordered_14d AS INT64 ) units,
        SAFE_CAST( attributed_sales_14d AS FLOAT64 ) revenue,

    FROM

        {{ref('dpl_legacy_amazon-ad_performance-sponsored_displaytargeting-v1')}}
)

SELECT * FROM amazon_target_union
