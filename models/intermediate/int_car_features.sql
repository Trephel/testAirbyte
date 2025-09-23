{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

SELECT
    source_id,
    advertisement_url,
    city,
    sector,
    brand,
    model,
    model_year,
    mileage_km,
    fuel_type,
    fiscal_power,
    transmission,
    doors_count,
    origin,
    first_hand,
    condition,
    price_dh,
    
    -- Calcul de l'âge du véhicule
    EXTRACT(YEAR FROM CURRENT_DATE) - model_year as car_age_years,
    
    -- Catégorisation par prix
    CASE 
        WHEN price_dh < 50000 THEN 'Economique (<50k DH)'
        WHEN price_dh BETWEEN 50000 AND 100000 THEN 'Moyenne gamme (50k-100k DH)'
        WHEN price_dh BETWEEN 100000 AND 200000 THEN 'Gamme supérieure (100k-200k DH)'
        WHEN price_dh BETWEEN 200000 AND 500000 THEN 'Premium (200k-500k DH)'
        ELSE 'Luxe (>500k DH)'
    END as price_category,
    
    -- Score d'équipements (nombre d'options)
    CAST(
        CASE WHEN alloy_wheels THEN 1 ELSE 0 END +
        CASE WHEN airbags THEN 1 ELSE 0 END +
        CASE WHEN air_conditioning THEN 1 ELSE 0 END +
        CASE WHEN gps_navigation THEN 1 ELSE 0 END +
        CASE WHEN sunroof THEN 1 ELSE 0 END +
        CASE WHEN leather_seats THEN 1 ELSE 0 END +
        CASE WHEN parking_sensor THEN 1 ELSE 0 END +
        CASE WHEN rear_camera THEN 1 ELSE 0 END +
        CASE WHEN electric_windows THEN 1 ELSE 0 END +
        CASE WHEN abs THEN 1 ELSE 0 END +
        CASE WHEN esp THEN 1 ELSE 0 END +
        CASE WHEN cruise_control THEN 1 ELSE 0 END +
        CASE WHEN multimedia_system THEN 1 ELSE 0 END +
        CASE WHEN onboard_computer THEN 1 ELSE 0 END +
        CASE WHEN remote_central_locking THEN 1 ELSE 0 END
    AS INTEGER) as equipment_score,
    
    -- Segments de véhicules
    CASE 
        WHEN brand IN ('Mercedes', 'BMW', 'Audi', 'Volvo') THEN 'Premium'
        WHEN brand IN ('Renault', 'Peugeot', 'Citroen', 'Dacia') THEN 'Français'
        WHEN brand IN ('Volkswagen', 'Opel', 'Ford') THEN 'Européen standard'
        WHEN brand IN ('Toyota', 'Nissan', 'Honda', 'Hyundai', 'Kia') THEN 'Japonais/Coréen'
        ELSE 'Autre'
    END as brand_segment,
    
    -- État simplifié
    CASE 
        WHEN condition ILIKE '%très bon%' OR condition ILIKE '%excellent%' THEN 'Très bon'
        WHEN condition ILIKE '%bon%' THEN 'Bon'
        WHEN condition ILIKE '%moyen%' OR condition ILIKE '%acceptable%' THEN 'Moyen'
        ELSE 'Non spécifié'
    END as simplified_condition,
    
    -- Tous les équipements booléens
    alloy_wheels,
    airbags,
    air_conditioning,
    gps_navigation,
    sunroof,
    leather_seats,
    parking_sensor,
    rear_camera,
    electric_windows,
    abs,
    esp,
    cruise_control,
    speed_limiter,
    multimedia_system,
    onboard_computer,
    remote_central_locking,
    
    loaded_at

FROM {{ ref('stg_avito_cars') }}
WHERE model_year IS NOT NULL
  AND price_dh IS NOT NULL