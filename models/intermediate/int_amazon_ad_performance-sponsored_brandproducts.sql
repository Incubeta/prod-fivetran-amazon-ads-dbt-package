

with report as (
    select * from {{ref('stg_sb_purchased_product')}}
),
exchange_rates as (
    select * from {{ ref('stg_openexchange_rates__openexchange_report_v1')}}
)
select 
    report.*,
    SAFE_CAST("0" as FLOAT64) / exchange_rates.ex_rate as _gbp_cost,
    report.sales_14d / exchange_rates.ex_rate as _gbp_revenue
from report
left join exchange_rates
on report.day = exchange_rates.day
and lower(ifnull(report.campaign_budget_currency_code, '{{var('account_currency')}}')) = exchange_rates.currency_code

