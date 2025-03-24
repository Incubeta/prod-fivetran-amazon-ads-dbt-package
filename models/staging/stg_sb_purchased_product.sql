with report as (
    select 
        product_name,
        attribution_type,
        new_to_brand_purchases_14_d as new_to_brand_purchases_14d,
        new_to_brand_sales_14_d as new_to_brand_sales_14d,
        new_to_brand_units_sold_14_d as new_to_brand_units_sold_14d,
        product_category,
        sales_14_d as sales_14d,
        purchased_asin,
        campaign_budget_currency_code,
        campaign_id,
        ad_group_id,
        units_sold_14_d as units_sold_14d,
        orders_14_d as orders_14d,
        SAFE_CAST(date as date) as day 
    from {{ source('dbt_amazon_ads','sb_purchased_product')}}

)
select * from report
