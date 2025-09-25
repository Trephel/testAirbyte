{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Test simple pour voir les premières données
SELECT 
    _airbyte_ab_id,
    _airbyte_emitted_at,
    _airbyte_data:Unnamed_0::string as test_id,
    _airbyte_data:Ville::string as test_city,
    _airbyte_data:Marque::string as test_brand,
    _airbyte_data:Prix::number as test_price
FROM AIRBYTE_DATABASE.AIRBYTE_SCHEMA."_AIRBYTE_RAW_AVITOCARDATASET"
LIMIT 5