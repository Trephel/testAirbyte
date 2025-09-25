{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'avito']
    )
}}

SELECT 
    _airbyte_ab_id as airbyte_id,
    _airbyte_emitted_at as loaded_at,
    
    -- Nettoyage des IDs
    CASE 
        WHEN _airbyte_data:Unnamed_0::string IS NOT NULL 
        THEN _airbyte_data:Unnamed_0::string 
        ELSE _airbyte_ab_id 
    END as source_id,
    
    -- Nettoyage des marques
    CASE 
        WHEN _airbyte_data:Marque::string IS NULL THEN 'Unknown'
        ELSE _airbyte_data:Marque::string 
    END as brand,
    
    _airbyte_data:Lien::string as advertisement_url,
    _airbyte_data:Ville::string as city,
    _airbyte_data:Secteur::string as sector,
    _airbyte_data:Modele::string as model,
    _airbyte_data:"Année-Modèle"::string as model_year_raw,
    _airbyte_data:"Kilométrage"::string as mileage_raw,
    _airbyte_data:"Type de carburant"::string as fuel_type,
    _airbyte_data:"Boite de vitesses"::string as transmission,
    _airbyte_data:"Nombre de portes"::string as doors_count,
    _airbyte_data:Prix::number as price_dh
    
FROM AIRBYTE_DATABASE.AIRBYTE_SCHEMA."_AIRBYTE_RAW_AVITOCARDATASET"
WHERE _airbyte_data:Prix::number IS NOT NULL
  AND _airbyte_data:Prix::number > 0  -- Prix valides