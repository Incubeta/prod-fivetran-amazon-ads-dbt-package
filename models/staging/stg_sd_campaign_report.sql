with report as (
    select 
        campaign_id,
        units_sold,
        impressions,
        clicks,
        purchases_clicks,
        sales_clicks,
        TRIM(campaign_budget_currency_code) as currency,
        SAFE_CAST(date as date) as day,
        SAFE_CAST(cost as FLOAT64) as cost,
                SAFE_CAST(null as FLOAT64) attributed_sales,
                SAFE_CAST(null AS FLOAT64) attributed_conversions
        from {{source('dbt_amazon_ads', 'sd_campaign_report')}}

        
        
        

)
select * from report
