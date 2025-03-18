with report as (
    select 
        campaign_id,
        units_sold_14_d as units_sold_14d,
        impressions,
        clicks,
        TRIM(currency) as currency,
        SAFE_CAST(attributed_sales_14_d as FLOAT64) as attributed_sales_14d,
        attributed_conversions_14_d as attributed_conversions_14d,
        report_date as day,
        SAFE_CAST(cost as FLOAT64) as cost,
        from {{source('dbt_amazon_ads', 'sb_campaign_report')}}

        
        
        

)
select * from report
