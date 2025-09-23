-- Test des années de modèle invalides
SELECT COUNT(*) as invalid_model_years
FROM {{ ref('stg_avito_cars') }}
WHERE model_year < 1990 OR model_year > 2024

UNION ALL

-- Test des kilométrages extrêmes
SELECT COUNT(*) as extreme_mileage
FROM {{ ref('stg_avito_cars') }}
WHERE mileage_km > 500000

UNION ALL

-- Test de cohérence âge/kilométrage
SELECT COUNT(*) as inconsistent_age_mileage
FROM {{ ref('int_car_features') }}
WHERE car_age_years > 0 AND (mileage_km / car_age_years) > 100000