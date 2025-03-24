with product_ad_history as (
    select id, asin, campaign_id, ad_group_id
    from {{source('dbt_amazon_ads', 'sd_product_ad_history')}}
    QUALIFY row_number() over(partition by id order by last_updated_date desc)=1 
)

select * from product_ad_history 
