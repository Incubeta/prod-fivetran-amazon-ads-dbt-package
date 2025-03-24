with report as (
    select * from {{ref('base_targeting_report_union_targeting_keyword_report')}}
)

select 
    report.targeting,
    report.units_sold_clicks_7_d as units_sold_clicks_7d,
    report.units_sold_same_sku_1_d as units_sold_same_sku_1d,
    report.sales_7_d as sales_7d,
    report.purchases_same_sku_7_d as purchases_same_sku_7d,
    report.attributed_sales_same_sku_30_d as attributed_sales_same_sku_30d,
    report.units_sold_other_sku_7_d as units_sold_other_sku_7d,
    report.units_sold_same_sku_14_d as units_sold_same_sku_14d,
    report.sales_14_d as sales_14d,
    report.units_sold_same_sku_7_d as units_sold_same_sku_7d,
    report.units_sold_same_sku_7_d as units_sold_same_sku_d, --the old model used this field name
    report.match_type ,
    SAFE_CAST(report.keyword_id as STRING) as keyword_id,
    SAFE_CAST(report.ad_group_id as STRING) as adgroup_id,
    report.sales_30_d as sales_30d,
    report.clicks ,
    report.purchases_same_sku_14_d purchases_same_sku_14d,
    report.purchases_1_d purchases_1d,
    report.attributed_sales_same_sku_7_d as attributed_sales_same_sku_7d,
   SAFE_CAST(report.campaign_id as STRING) as campaign_id,
    report.sales_1_d as sales_1d,
    report.campaign_budget_currency_code  as currency,
    report.units_sold_clicks_30_d as units_sold_clicks_30d,
    report.keyword_type ,
    report.units_sold_clicks_14_d units_sold_clicks_14d,
    report.purchases_7_d as purchases_7d,
    report.attributed_sales_same_sku_1_d as attributed_sales_same_sku_1d,
    report.purchases_14_d as purchases_14d,
    report.impressions ,
    report.cost ,
    report.purchases_same_sku_1_d as purchases_same_sku_1d,
    report.purchases_30_d as purchases_30d,
    report.attributed_sales_same_sku_14_d attributed_sales_same_sku_14d,
    SAFE_CAST( report.date AS DATE ) day,
    report.units_sold_same_sku_30_d as units_sold_same_sku_30d,
    report.units_sold_clicks_1_d as units_sold_clicks_1d,
    report.purchases_same_sku_30_d as purchases_same_sku_30d
from report
