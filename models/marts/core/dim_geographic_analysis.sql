{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

SELECT
    city,
    sector,
    
    -- Statistiques par ville
    COUNT(*) as total_ads_city,
    AVG(price_dh) as avg_price_city,
    AVG(mileage_km) as avg_mileage_city,
    AVG(car_age_years) as avg_age_city,
    
    -- Répartition par marque dans la ville
    COUNT(DISTINCT brand) as distinct_brands_count,
    
    -- Top marques par ville
    MODE(brand) as most_popular_brand,
    MODE(fuel_type) as most_popular_fuel_type,
    
    -- Équipements par ville
    AVG(CASE WHEN air_conditioning THEN 1 ELSE 0 END) * 100 as ac_percentage_city,
    AVG(CASE WHEN airbags THEN 1 ELSE 0 END) * 100 as airbags_percentage_city,
    
    -- Prix moyens par segment
    AVG(CASE WHEN brand_segment = 'Premium' THEN price_dh END) as avg_premium_price,
    AVG(CASE WHEN brand_segment = 'Français' THEN price_dh END) as avg_french_price,
    AVG(CASE WHEN brand_segment = 'Japonais/Coréen' THEN price_dh END) as avg_asian_price

FROM {{ ref('int_car_features') }}
GROUP BY city, sector
HAVING total_ads_city >= 5  -- Villes avec suffisamment d'annonces