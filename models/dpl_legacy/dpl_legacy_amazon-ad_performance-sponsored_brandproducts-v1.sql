
with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
/* select statement when the first_run is 0 and days_back values are passed in */
SELECT DISTINCT
    SAFE_CAST( profileBrandName AS STRING ) profile_brand_name,
    SAFE_CAST( productName AS STRING ) product_name,
    SAFE_CAST( campaignName AS STRING ) campaign_name,
    SAFE_CAST( attributionType AS STRING ) attribution_type,
    SAFE_CAST( newToBrandPurchases14d AS INT64 ) new_to_brand_purchases_14d,
    SAFE_CAST( adGroupName AS STRING ) adgroup_name,
    SAFE_CAST( newToBrandSales14d AS FLOAT64 ) new_to_brand_sales_14d,
    SAFE_CAST( productCategory AS STRING ) product_category,
    SAFE_CAST( sales14d AS FLOAT64 ) sales_14d,
    SAFE_CAST( profileCountryCode AS STRING ) profile_country_code,
    SAFE_CAST( purchasedAsin AS STRING ) purchased_asin,
    SAFE_CAST( TRIM(campaignBudgetCurrencyCode) AS STRING ) campaign_budget_currency_code,
    SAFE_CAST( profileId AS STRING ) profile_id,
    SAFE_CAST( unitsSold14d AS INT64 ) units_sold_14d,
    SAFE_CAST( orders14d AS INT64 ) orders_14d,
    SAFE_CAST( date AS DATE ) day,
    SAFE_CAST( newToBrandUnitsSold14d AS INT64 ) new_to_brand_units_sold_14d,

    (SAFE_CAST("0" AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
    (SAFE_CAST(sales14d AS FLOAT64) / source_b.ex_rate) _gbp_revenue , -- using AllRevenue column,
    'amazon-ad_performance-sponsored_brandproducts-v1' AS raw_origin,
    {{ add_fields("campaignName") }} /* Replace with the report's campaign name field */

FROM
    {{ref('raw_main_amazon-ad_performance-sponsored_brandproducts-v1')}} source_a
LEFT JOIN
left join exchange_rates
on SAFE_CAST(source_a.date as DATE) = exchange_rates.day
AND

    LOWER(IFNULL(TRIM(campaignBudgetCurrencyCode),'{{ var('account_currency') }}')) = source_b.currency_code

