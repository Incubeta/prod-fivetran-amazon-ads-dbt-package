with campaigns as (
    select 
        id as campaign_id, 
        name as campaign_name,
        state as campaign_status,
        budget_type as campaign_budget_type,
        profile_id,
        targeting_type
        
        from {{source('dbt_amazon_ads', 'campaign_history') }}
        QUALIFY ROW_NUMBER() over(partition by id order by last_updated_date desc)=1
)
select * from campaigns
