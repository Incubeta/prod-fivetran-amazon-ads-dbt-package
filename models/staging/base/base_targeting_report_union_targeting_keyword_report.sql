{{
dbt_utils.union_relations(relations=[source('dbt_amazon_ads', 'targeting_report'), source('dbt_amazon_ads', 'targeting_keyword_report')])
}}
