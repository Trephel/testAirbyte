{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

SELECT
    city,
    sector,
    
    -- Statistiques de base
    COUNT(*) as total_ads_city,
    AVG(price_dh) as avg_price_city,
    AVG(mileage_km) as avg_mileage_city,
    
    -- Calcul de l'âge moyen des véhicules (si l'année est disponible)
    AVG(EXTRACT(YEAR FROM CURRENT_DATE()) - model_year) as avg_age_city,
    
    -- Répartition par marque
    COUNT(DISTINCT brand) as distinct_brands_count,
    
    -- Marque et carburant les plus populaires (version simplifiée)
    MAX(brand) as most_popular_brand,  -- Remplace MODE temporairement
    MAX(fuel_type) as most_popular_fuel_type
    
FROM {{ ref('int_car_features') }}
WHERE price_dh IS NOT NULL
GROUP BY city, sector
HAVING total_ads_city >= 3  -- Villes avec suffisamment d'annonces