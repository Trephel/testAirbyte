{{
    config(
        materialized='table',
        schema='analytics',
        tags=['reporting', 'market_analysis']
    )
}}

WITH market_metrics AS (
    SELECT
        -- Dimensions principales
        brand,
        model,
        city,
        fuel_type,
        transmission,
        brand_segment,
        price_category,
        simplified_condition,
        
        -- Agrégations de base
        COUNT(*) as total_ads,
        AVG(price_dh) as avg_price,
        AVG(mileage_km) as avg_mileage,
        AVG(car_age_years) as avg_age,
        AVG(equipment_score) as avg_equipment_score,
        
        -- Métriques par état
        COUNT(CASE WHEN first_hand THEN 1 END) as first_hand_count,
        COUNT(CASE WHEN condition ILIKE '%très bon%' THEN 1 END) as excellent_condition_count,
        
        -- Métriques équipements
        COUNT(CASE WHEN air_conditioning THEN 1 END) as with_ac_count,
        COUNT(CASE WHEN airbags THEN 1 END) as with_airbags_count,
        COUNT(CASE WHEN abs THEN 1 END) as with_abs_count,
        COUNT(CASE WHEN leather_seats THEN 1 END) as with_leather_seats_count,
        
        -- Prix extrêmes
        MIN(price_dh) as min_price,
        MAX(price_dh) as max_price,
        MIN(mileage_km) as min_mileage,
        MAX(mileage_km) as max_mileage
        
    FROM {{ ref('int_car_features') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT
    *,
    
    -- KPI calculés
    first_hand_count / NULLIF(total_ads, 0) * 100 as first_hand_percentage,
    excellent_condition_count / NULLIF(total_ads, 0) * 100 as excellent_condition_percentage,
    with_ac_count / NULLIF(total_ads, 0) * 100 as ac_percentage,
    with_airbags_count / NULLIF(total_ads, 0) * 100 as airbags_percentage,
    
    -- Ratio prix/âge
    avg_price / NULLIF(avg_age, 0) as price_per_year,
    
    -- Ratio prix/équipements
    avg_price / NULLIF(avg_equipment_score, 0) as price_per_equipment_point,
    
    -- Différence prix max/min
    max_price - min_price as price_range

FROM market_metrics
WHERE total_ads >= 3  -- Ignorer les segments avec peu d'annonces