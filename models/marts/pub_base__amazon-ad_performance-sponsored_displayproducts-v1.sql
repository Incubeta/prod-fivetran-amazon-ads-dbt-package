with campaigns as (
    select * from {{ref('stg_sd_campaign_history') }}

),
profiles as (
    select * from {{ ref('stg_profile') }}
),
product_ad_history as (
    select * from {{ref('stg_sd_product_ad_history')}}
),
exchange_rates as (
    select * from {{ ref('stg_openexchange_rates__openexchange_report_v1')}}
),
report as (
    select * from {{ref('stg_sd_product_ad_report') }}
)
SELECT 
    campaigns.campaign_name,
    profiles.profile_brand_name,
    profiles.profile_currency_code as currency,
    profiles.profile_country_code,
    SAFE_CAST(profiles.id as STRING) as profile_id,
    product_ad_history.asin as product_ads_asin,
    report.* except(ad_group_id),
    report.cost / exchange_rates.ex_rate as _gbp_cost,
    report.attributed_sales_30d / exchange_rates.ex_rate as _gbp_revenue_30d,
    report.attributed_sales_14d / exchange_rates.ex_rate as _gbp_revenue_14d,
    report.attributed_sales_7d / exchange_rates.ex_rate as _gbp_revenue_7d,
    report.attributed_sales_1d / exchange_rates.ex_rate as _gbp_revenue_1d,
    {{add_fields('campaigns.campaign_name')}}
from report

left join campaigns
    on report.campaign_id = campaigns.campaign_id
left join profiles
    on campaigns.profile_id = profiles.id
left join exchange_rates
on report.day = exchange_rates.day
and lower(ifnull(profiles.profile_currency_code, '{{var('account_currency')}}')) = exchange_rates.currency_code
left join product_ad_history
    on product_ad_history.campaign_id = report.campaign_id
    and product_ad_history.ad_group_id = report.ad_group_id
    and product_ad_history.id = report.adId

