{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'avito']
    )
}}

WITH raw_data AS (
    SELECT 
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data
    FROM AIRBYTE_DATABASE.AIRBYTE_SCHEMA."_AIRBYTE_RAW_AVITOCARDATASET"
),

extracted_data AS (
    SELECT 
        _airbyte_ab_id as airbyte_id,
        _airbyte_emitted_at as loaded_at,
        
        -- Identifiants et URLs
        _airbyte_data:Unnamed_0::string as source_id,
        _airbyte_data:Lien::string as advertisement_url,
        
        -- Localisation
        _airbyte_data:Ville::string as city,
        _airbyte_data:Secteur::string as sector,
        
        -- Informations véhicule
        _airbyte_data:Marque::string as brand,
        _airbyte_data:Modele::string as model,
        _airbyte_data:"Année-Modèle"::string as model_year_raw,
        _airbyte_data:"Kilométrage"::string as mileage_raw,
        
        -- Caractéristiques techniques
        _airbyte_data:"Type de carburant"::string as fuel_type,
        _airbyte_data:"Puissance fiscale"::string as fiscal_power,
        _airbyte_data:"Boite de vitesses"::string as transmission,
        _airbyte_data:"Nombre de portes"::string as doors_count,
        
        -- Prix
        _airbyte_data:Prix::number as price_dh,
        
        -- État et historique
        _airbyte_data:"Première main"::string as first_hand_raw,
        _airbyte_data:Etat::string as condition
        
    FROM raw_data
)

SELECT *
FROM extracted_data
WHERE price_dh IS NOT NULL
  AND brand IS NOT NULL