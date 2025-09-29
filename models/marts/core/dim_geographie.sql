{{
    config(
        materialized='table',
        alias='dim_geographie'
    )
}}

WITH geographie_base AS (
    SELECT 
        ville,
        secteur,
        origine,
        -- Nettoyage région
        CASE 
            WHEN UPPER(ville) LIKE '%PARIS%' THEN 'Ile-de-France'
            WHEN UPPER(ville) LIKE '%LYON%' THEN 'Auvergne-Rhône-Alpes'
            WHEN UPPER(ville) LIKE '%MARSEILLE%' THEN 'Provence-Alpes-Côte d''Azur'
            ELSE 'Autre'
        END as region
        
    FROM {{ ref('avitocardsataset') }}
    WHERE ville IS NOT NULL
),

geographie_dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ville, secteur 
            ORDER BY ville
        ) as rn
    FROM geographie_base
)

SELECT
    MD5(CONCAT(COALESCE(ville, ''), COALESCE(secteur, ''))) as geographie_id,
    ville,
    secteur,
    origine,
    region
    
FROM geographie_dedup
WHERE rn = 1