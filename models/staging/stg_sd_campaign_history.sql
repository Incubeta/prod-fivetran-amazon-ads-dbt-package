with campaigns as (
    select 
        id as campaign_id, 
        name as campaign_name,
        state as campaign_status,
        cost_type as campaign_type,
        budget_type as campaign_budget_type,
        profile_id
        from {{source('dbt_amazon_ads', 'sd_campaign_history') }}
        QUALIFY ROW_NUMBER() over(partition by id order by last_updated_date desc)=1
)
select * from campaigns
