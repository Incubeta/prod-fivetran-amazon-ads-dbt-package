with profiles as (
    select 
        id,
        account_name as profile_brand_name,
        country_code as profile_country_code,
        TRIM(currency_code) as profile_currency_code,
    from {{source('dbt_amazon_ads', 'profile')}}
)
select * from profiles
