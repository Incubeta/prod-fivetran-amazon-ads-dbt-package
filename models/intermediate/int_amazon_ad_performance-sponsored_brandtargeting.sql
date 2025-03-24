

with report as (
    select * from {{ref('stg_sb_keyword_report')}}
),
exchange_rates as (
    select * from {{ ref('stg_openexchange_rates__openexchange_report_v1')}}
)
select 
    report.*,
    SAFE_CAST(report.cost as FLOAT64) / exchange_rates.ex_rate as _gbp_cost,
    report.attributed_sales_14d / exchange_rates.ex_rate as _gbp_revenue
from report
left join exchange_rates
on report.day = exchange_rates.day
and lower(ifnull(report.currency, '{{var('account_currency')}}')) = exchange_rates.currency_code

