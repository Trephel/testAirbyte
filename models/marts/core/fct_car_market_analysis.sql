{{
    config(
        materialized='table',
        schema='analytics',
        tags=['marts', 'core', 'reporting']
    )
}}

SELECT 
    -- Vos métriques business
    city,
    brand,
    COUNT(*) as total_ads
FROM {{ ref('stg_avito_cars') }}
GROUP BY city, brand