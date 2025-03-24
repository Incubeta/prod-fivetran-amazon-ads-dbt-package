with report as (
    select 
        SAFE_CAST(report_date as date) as day,
        ad_group_id,
        attributed_conversions_14_d as attributed_conversions_14d,
        attributed_sales_14_d as attributed_sales_14d,
        campaign_id,
        clicks,
        cost,
        currency,
        impressions,
        attributed_conversions_14_d_same_sku as attributed_conversions_14d_same_sku,
        attributed_sales_14_d_same_sku as attributed_sales_14d_same_sku,
        SAFE_CAST(search_term_impression_rank as STRING) as search_term_impression_rank,
        top_of_search_impression_share,
        SAFE_CAST(search_term_impression_share as STRING) as search_term_impression_share,
        units_sold_14_d as units_sold_14d,
        keyword_id
    from {{ source('dbt_amazon_ads', 'sb_keyword_report')}}
        
)
select * from report
