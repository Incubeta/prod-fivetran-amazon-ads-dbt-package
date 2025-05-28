
with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
/* select statement when the first_run is 0 and days_back values are passed in */
SELECT DISTINCT
    SAFE_CAST( unitsSoldClicks7d AS INT64 ) units_sold_clicks_7d,
    SAFE_CAST( unitsSoldSameSku1d AS INT64 ) units_sold_same_sku_1d,
    SAFE_CAST( sales7d AS FLOAT64 ) sales_7d,
    SAFE_CAST( purchasesSameSku7d AS INT64 ) purchases_same_sku_7d,
    SAFE_CAST( attributedSalesSameSku30d AS FLOAT64 ) attributed_sales_same_sku_30d,
    SAFE_CAST( spend AS FLOAT64 ) spend,
    SAFE_CAST( unitsSoldOtherSku7d AS INT64 ) units_sold_other_sku_7d,
    SAFE_CAST( campaignName AS STRING ) campaign_name,
    SAFE_CAST( adId AS STRING ) ad_id,
    SAFE_CAST( adGroupId AS STRING ) adgroup_id,
    SAFE_CAST( unitsSoldSameSku14d AS INT64 ) units_sold_same_sku_14d,
    SAFE_CAST( sales14d AS FLOAT64 ) sales_14d,
    SAFE_CAST( unitsSoldSameSku7d AS INT64 ) units_sold_same_sku_7d,
    SAFE_CAST( profileCountryCode AS STRING ) profile_country_code,
    SAFE_CAST( advertisedAsin AS STRING ) advertised_asin,
    SAFE_CAST( profileId AS STRING ) profile_id,
    SAFE_CAST( sales30d AS FLOAT64 ) sales_30d,
    SAFE_CAST( clicks AS INT64 ) clicks,
    SAFE_CAST( purchasesSameSku14d AS INT64 ) purchases_same_sku_14d,
    SAFE_CAST( purchasesSameSku30d AS INT64 ) purchases_same_sku_30d,
    SAFE_CAST( purchases1d AS INT64 ) purchases_1d,
    SAFE_CAST( attributedSalesSameSku7d AS FLOAT64 ) attributed_sales_same_sku_7d,
    SAFE_CAST( campaignId AS STRING ) campaign_id,
    SAFE_CAST( profileBrandName AS STRING ) profile_brand_name,
    SAFE_CAST( sales1d AS FLOAT64 ) sales_1d,
    SAFE_CAST( TRIM(currency) AS STRING ) currency,
    SAFE_CAST( unitsSoldClicks30d AS INT64 ) units_sold_clicks_30d,
    SAFE_CAST( keywordType AS STRING ) keyword_type,
    SAFE_CAST( unitsSoldClicks14d AS INT64 ) units_sold_Clicks_14d,
    SAFE_CAST( purchases7d AS INT64 ) purchases_7d,
    SAFE_CAST( attributedSalesSameSku1d AS FLOAT64 ) attributed_sales_same_sku_1d,
    SAFE_CAST( purchases14d AS INT64 ) purchases_14d,
    SAFE_CAST( impressions AS INT64 ) impressions,
    SAFE_CAST( adGroupName AS STRING ) adgroup_name,
    SAFE_CAST( cost AS FLOAT64 ) cost,
    SAFE_CAST( purchasesSameSku1d AS INT64 ) purchases_same_sku_1d,
    SAFE_CAST( purchases30d AS INT64 ) purchases_30d,
    SAFE_CAST( attributedSalesSameSku14d AS FLOAT64 ) attributed_sales_same_sku_14d,
    SAFE_CAST( date AS DATE ) day,
    SAFE_CAST( unitsSoldSameSku30d AS INT64 ) units_sold_same_sku_30d,
    SAFE_CAST( unitsSoldClicks1d AS INT64 ) units_sold_clicks_1d,
    SAFE_CAST( salesOtherSku7d AS FLOAT64 ) sales_other_sku_7d,
    SAFE_CAST( campaignType AS STRING ) campaign_type,

    (SAFE_CAST(cost AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
    (SAFE_CAST(sales1d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_1d ,
    (SAFE_CAST(sales7d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_7d,
    (SAFE_CAST(sales14d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_14d ,
    (SAFE_CAST(sales30d AS FLOAT64) / source_b.ex_rate) _gbp_revenue_30d ,

    'amazon-ad_performance-sponsored_productproducts-v1' AS raw_origin,
    {{ add_fields("campaignName") }} /* Replace with the report's campaign name field */

FROM
    {{ref('raw_main_amazon-ad_performance-sponsored_productproducts-v1')}} source_a
LEFT JOIN
left join exchange_rates
on SAFE_CAST(source_a.date as DATE) = exchange_rates.day
AND

    LOWER(IFNULL(TRIM(currency),'{{ var('account_currency') }}')) = source_b.currency_code

