{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- dbt:autocompletion:enable
WITH source_data AS (
    SELECT 
        "Unnamed: 0" as source_id,
        "Lien" as advertisement_url,
        "Ville" as city,
        "Secteur" as sector,
        "Marque" as brand,
        "Modèle" as model,
        "Année-Modèle" as model_year_raw,
        "Kilométrage" as mileage_raw,
        "Type de carburant" as fuel_type,
        "Puissance fiscale" as fiscal_power,
        "Boite de vitesses" as transmission,
        "Nombre de portes" as doors_count,
        "Origine" as origin,
        "Première main" as first_hand_raw,
        "État" as condition,
        "Prix" as price_raw
    FROM {{ source('airbyte_raw', 'avito_car_dataset') }}
),

cleaned_data AS (
    SELECT
        source_id,
        advertisement_url,
        INITCAP(TRIM(city)) as city,
        INITCAP(TRIM(sector)) as sector,
        INITCAP(TRIM(brand)) as brand,
        INITCAP(TRIM(model)) as model,
        
        -- Nettoyage année
        CASE 
            WHEN model_year_raw ~ '^\\d{4}$' 
            THEN CAST(model_year_raw AS INTEGER)
            ELSE NULL 
        END as model_year,
        
        -- Nettoyage kilométrage
        CASE 
            WHEN mileage_raw ~ '\\d+\\s*-\\s*\\d+' THEN
                (
                    CAST(SPLIT_PART(SPLIT_PART(mileage_raw, '-', 1), ' ', 1) AS INTEGER) +
                    CAST(REGEXP_REPLACE(SPLIT_PART(mileage_raw, '-', 2), '[^0-9]', '', 'g') AS INTEGER)
                ) / 2
            WHEN mileage_raw ~ '^\\d+$' THEN CAST(mileage_raw AS INTEGER)
            ELSE NULL 
        END as mileage_km,
        
        INITCAP(TRIM(fuel_type)) as fuel_type,
        
        CASE 
            WHEN fiscal_power ~ '^\\d+$' THEN CAST(fiscal_power AS INTEGER)
            ELSE NULL 
        END as fiscal_power,
        
        INITCAP(TRIM(transmission)) as transmission,
        
        CASE 
            WHEN doors_count ~ '^\\d+$' THEN CAST(doors_count AS INTEGER)
            ELSE NULL 
        END as doors_count,
        
        INITCAP(TRIM(origin)) as origin,
        
        CASE 
            WHEN first_hand_raw = 'Oui' THEN TRUE
            WHEN first_hand_raw = 'Non' THEN FALSE
            ELSE NULL 
        END as first_hand,
        
        INITCAP(TRIM(condition)) as condition,
        
        CASE 
            WHEN price_raw ~ '^\\d+$' THEN CAST(price_raw AS INTEGER)
            ELSE NULL 
        END as price_dh,
        
        CURRENT_TIMESTAMP as loaded_at
        
    FROM source_data
)

SELECT *
FROM cleaned_data
WHERE brand IS NOT NULL
  AND model_year BETWEEN 1990 AND EXTRACT(YEAR FROM CURRENT_DATE)
  AND price_dh BETWEEN 1000 AND 2000000