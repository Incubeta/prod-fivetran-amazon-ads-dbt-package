with report as (
    select * from {{source('dbt_amazon_ads', 'sd_target_report')}}
)

select 
    SAFE_CAST(report.date as date) as day,
    report.campaign_id,
    report.ad_group_id,
    report.ad_id as adId,
    SAFE_CAST(report.video_complete_views as INT64) as video_complete_views,
    SAFE_CAST(report.video_unmutes as INT64) as video_unmutes,
    SAFE_CAST(report.video_third_quartile_views as INT64) as video_third_quartile_views,
    SAFE_CAST(report.video_midpoint_views as INT64) as video_midpoint_views,
    SAFE_CAST(report.video_first_quartile_views as INT64) as video_first_quartile_views,
    report.impressions,
    report.clicks,
    report.cost,
    report.sales_clicks,
    report.impressions_views as view_impressions,
    report.targeting_text,
SAFE_CAST(null as INT64) as attributed_conversions_14d,
SAFE_CAST( null as FLOAT64) as attributed_conversions_14d_same_sku,
	SAFE_CAST(null as INT64) as attributed_conversions_1d,
	SAFE_CAST(null as INT64) as attributed_conversions_1d_same_sku,
	SAFE_CAST(null as INT64) as attributed_conversions_30d,
	SAFE_CAST(null as INT64) as attributed_conversions_30d_same_sku,
	SAFE_CAST(null as INT64) as attributed_conversions_7d,
	SAFE_CAST(null as INT64) as attributed_conversions_7d_same_sku,
	SAFE_CAST(null as INT64) as attributed_orders_new_to_brand_14d,
	SAFE_CAST(null as FLOAT64) as attributed_sales_14d,
	SAFE_CAST(null as FLOAT64) as attributed_sales_14d_same_sku,
	SAFE_CAST(null as FLOAT64) as attributed_sales_1d,
	SAFE_CAST(null as FLOAT64) as attributed_sales_1d_same_sku,
	SAFE_CAST(null as FLOAT64) as attributed_sales_30d,
	SAFE_CAST(null as FLOAT64) as attributed_sales_30d_same_sku,
	SAFE_CAST(null as FLOAT64) as attributed_sales_7d,
	SAFE_CAST(null as FLOAT64) as attributed_sales_7d_same_sku,
	SAFE_CAST(null as FLOAT64) as attributed_sales_new_to_brand_14d,
	SAFE_CAST(null as FLOAT64) as attributed_units_ordered_1d,
	SAFE_CAST(null as FLOAT64) as attributed_units_ordered_14d,
	SAFE_CAST(null as FLOAT64) as attributed_units_ordered_30d,
	SAFE_CAST(null as FLOAT64) as attributed_units_ordered_7d,
	SAFE_CAST(null as FLOAT64) as attributed_units_ordered_new_to_brand_14d,
	"TARGETING_EXPRESSION" as targeting_type
from report



