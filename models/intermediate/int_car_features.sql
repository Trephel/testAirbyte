{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

SELECT
    source_id,
    city,
    sector,
    brand,
    model,
    model_year_raw as model_year,
    
    -- Kilométrage temporairement désactivé
    NULL as mileage_km,
    
    fuel_type,
    transmission,
    price_dh,
    
    -- Calcul de l'âge du véhicule
    CASE 
        WHEN model_year_raw IS NOT NULL 
        THEN EXTRACT(YEAR FROM CURRENT_DATE()) - model_year_raw 
        ELSE NULL 
    END as car_age_years
    
FROM {{ ref('stg_avito_cars') }}
WHERE price_dh IS NOT NULL
  AND brand IS NOT NULL