with keyword_history as (
    select 
        id, 
        keyword_text
    from {{source('dbt_amazon_ads', 'keyword_history')}}
    QUALIFY row_number() over(partition by id order by last_updated_date desc )=1

)

select * from keyword_history
