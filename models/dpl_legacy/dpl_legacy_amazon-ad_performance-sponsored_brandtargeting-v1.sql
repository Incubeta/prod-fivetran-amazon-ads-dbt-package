
with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
/* select statement when the first_run is 0 and days_back values are passed in */
SELECT DISTINCT
    SAFE_CAST( TRIM(currency) AS STRING ) currency,
    SAFE_CAST( keywordText AS STRING ) keyword_text,
    SAFE_CAST( campaignStatus AS STRING ) campaign_status,
    SAFE_CAST( campaignName AS STRING ) campaign_name,
    SAFE_CAST( attributedConversions14dSameSKU AS INT64 ) attributed_conversions_14d_Same_sku,
    SAFE_CAST( adGroupId AS STRING ) adgroup_id,
    SAFE_CAST( searchTermImpressionRank AS STRING ) search_term_impression_rank,
    SAFE_CAST( topOfSearchImpressionShare AS FLOAT64 ) top_of_search_impression_share,
    SAFE_CAST( campaignType AS STRING ) campaign_type,
    SAFE_CAST( searchTermImpressionShare AS STRING ) search_term_impression_share,
    SAFE_CAST( impressions AS INT64 ) impressions,
    SAFE_CAST( adGroupName AS STRING ) adgroup_name,
    SAFE_CAST( cost AS FLOAT64 ) cost,
    SAFE_CAST( profileCountryCode AS STRING ) profile_country_code,
    SAFE_CAST( matchType AS STRING ) match_type,
    SAFE_CAST( attributedSales14dSameSKU AS FLOAT64 ) attributed_sales_14d_same_sku,
    SAFE_CAST( attributedSales14d AS FLOAT64 ) attributed_sales_14d,
    SAFE_CAST( clicks AS INT64 ) clicks,
    SAFE_CAST( unitsSold14d AS INT64 ) units_sold_14d,
    SAFE_CAST( campaignBudgetType AS STRING ) campaign_budget_type,
    SAFE_CAST( date AS DATE ) day,
    SAFE_CAST( attributedConversions14d AS INT64 ) attributed_conversions_14d,
    SAFE_CAST( campaignId AS STRING ) campaign_id,
    SAFE_CAST( profileBrandName AS STRING ) profile_brand_name,

    (SAFE_CAST(cost AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
    (SAFE_CAST(attributedSales14d AS FLOAT64) / source_b.ex_rate) _gbp_revenue ,

    'amazon-ad_performance-sponsored_brandtargeting-v1' AS raw_origin,
    {{ add_fields("campaignName") }} /* Replace with the report's campaign name field */

FROM
    {{ref('raw_main_amazon-ad_performance-sponsored_brandtargeting-v1')}} source_a
LEFT JOIN
left join exchange_rates
on SAFE_CAST(source_a.date as DATE) = exchange_rates.day
AND

    LOWER(IFNULL(TRIM(currency),'{{ var('account_currency') }}')) = source_b.currency_code

