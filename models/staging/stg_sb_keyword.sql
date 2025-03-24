with keywords as (
    select 
        id,
        keyword_text,
        match_type
    from {{ source('dbt_amazon_ads', 'sb_keyword')}}
)
select * from keywords
