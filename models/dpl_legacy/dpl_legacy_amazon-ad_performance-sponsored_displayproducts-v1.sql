
with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
/* select statement when the first_run is 0 and days_back values are passed in */
SELECT DISTINCT
    SAFE_CAST( date AS DATE ) day,
    SAFE_CAST( campaignId AS STRING ) campaign_id,
    SAFE_CAST( campaignName AS STRING ) campaign_name,
    SAFE_CAST( adId AS STRING ) adId,
    SAFE_CAST( profileCountryCode AS STRING ) profile_country_code,
    SAFE_CAST( profileId AS STRING ) profile_id,
    SAFE_CAST( profileBrandName AS STRING ) profile_brand_name,
    SAFE_CAST( TRIM(currency) AS STRING ) currency,
    SAFE_CAST( productAdsAsin AS STRING ) product_ads_asin,
    SAFE_CAST( attributedSales1dSameSKU AS FLOAT64 ) attributed_sales_1d_same_sku,
    SAFE_CAST( attributedUnitsOrdered7d AS INT64 ) attributed_units_ordered_7d,
    SAFE_CAST( attributedConversions7dSameSKU AS INT64 ) attributed_conversions_7d_same_sku,
    SAFE_CAST( videoCompleteViews AS INT64 ) video_complete_views,
    SAFE_CAST( attributedOrdersNewToBrand14d AS INT64 ) attributed_orders_new_to_brand_14d,
    SAFE_CAST( videoUnmutes AS INT64 ) video_unmutes,
    SAFE_CAST( attributedConversions14dSameSKU AS INT64 ) attributed_conversions_14d_same_sku,
    SAFE_CAST( videoThirdQuartileViews AS INT64 ) video_third_quartile_views,
    SAFE_CAST( attributedSales30dSameSKU AS FLOAT64 ) attributed_sales_30d_same_sku,
    SAFE_CAST( attributedConversions1dSameSKU AS INT64 ) attributed_conversions_1d_same_sku,
    SAFE_CAST( attributedSales1d AS FLOAT64 ) attributed_sales_1d,
    SAFE_CAST( attributedSales14d AS FLOAT64 ) attributed_sales_14d,
    SAFE_CAST( attributedSales14dSameSKU AS FLOAT64 ) attributed_sales_14d_same_sku,
    SAFE_CAST( attributedConversions7d AS INT64 ) attributed_conversions_7d,
    SAFE_CAST( attributedUnitsOrdered1d AS INT64 ) attributed_units_ordered_1d,
    SAFE_CAST( attributedUnitsOrdered30d AS INT64 ) attributed_units_ordered_30d,
    SAFE_CAST( videoMidpointViews AS INT64 ) video_midpoint_views,
    SAFE_CAST( attributedUnitsOrderedNewToBrand14d AS INT64 ) attributed_units_ordered_new_to_brand_14d,
    SAFE_CAST( viewImpressions AS INT64 ) view_impressions,
    SAFE_CAST( videoFirstQuartileViews AS INT64 ) video_first_quartile_views,
    SAFE_CAST( attributedSalesNewToBrand14d AS FLOAT64 ) attributed_sales_new_to_brand_14d,
    SAFE_CAST( attributedUnitsOrdered14d AS INT64 ) attributed_units_ordered_14d,
    SAFE_CAST( attributedConversions1d AS INT64 ) attributed_conversions_1d,
    SAFE_CAST( attributedSales30d AS FLOAT64 ) attributed_sales_30d,
    SAFE_CAST( attributedSales7dSameSKU AS FLOAT64 ) attributed_sales_7d_same_sku,
    SAFE_CAST( impressions AS INT64 ) impressions,
    SAFE_CAST( clicks AS INT64 ) clicks,
    SAFE_CAST( cost AS FLOAT64 ) cost,
    SAFE_CAST( attributedSales7d AS FLOAT64 ) attributed_sales_7d,
    SAFE_CAST( attributedConversions14d AS INT64 ) attributed_conversions_14d,
    SAFE_CAST( attributedConversions30dSameSKU AS INT64 ) attributed_conversions_30d_same_sku,
    SAFE_CAST( attributedConversions30d AS INT64 ) attributed_conversions_30d,
    (SAFE_CAST(cost AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
    (SAFE_CAST(attributedSales30d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_30d ,
    (SAFE_CAST(attributedSales14d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_14d ,
    (SAFE_CAST(attributedSales7d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_7d ,
    (SAFE_CAST(attributedSales1d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_1d ,

    'amazon-ad_performance-sponsored_displayproducts-v1' AS raw_origin,
    {{ add_fields("campaignName") }} /* Replace with the report's campaign name field */

FROM
    {{ref('raw_main_amazon_ad_performance-sponsored_displayproducts')}} source_a
left join exchange_rates
on SAFE_CAST(source_a.date as DATE) = exchange_rates.day
AND

    LOWER(IFNULL(TRIM(currency),'{{ var('account_currency') }}')) = source_b.currency_code

