with report as (
    select * from {{source('dbt_amazon_ads', 'campaign_level_report')}}
)
select 
    report.impressions,
    report.clicks,
    report.campaign_budget_currency_code,
    report.units_sold_clicks_14_d as units_sold_clicks_14d,
    report.purchases_14_d as purchases_14d,
    SAFE_CAST(report.date as DATE) as day,
    report.cost,
    report.sales_14d
from report
