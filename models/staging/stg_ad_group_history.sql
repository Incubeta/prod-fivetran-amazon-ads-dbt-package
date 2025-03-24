with adgroups as (
    select id as adgroup_id,
        name as adgroup_name,
    from {{ source('dbt_amazon_ads', 'ad_group_history') }}
    QUALIFY ROW_NUMBER() over(partition by id order by last_updated_date desc)=1
)
select * from adgroups
