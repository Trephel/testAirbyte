{{
    config(
        materialized='table',
        alias='dim_vehicules'
    )
}}

WITH vehicules_base AS (
    SELECT 
        _airbyte_ab_id as vehicule_key,
        marque,
        modele,
        annee_modele,
        type_carburant_clean as type_carburant,
        boite_vitesses_clean as boite_vitesses,
        puissance_fiscale_clean as puissance_fiscale,
        is_premiere_main,
        nombre_portes_clean as nombre_portes,
        -- Catégorie véhicule
        CASE 
            WHEN prix_euros < 10000 THEN 'Economique'
            WHEN prix_euros BETWEEN 10000 AND 25000 THEN 'Moyenne'
            WHEN prix_euros BETWEEN 25001 AND 50000 THEN 'Haut de gamme'
            ELSE 'Luxe'
        END as categorie_prix,
        -- Âge du véhicule
        CASE 
            WHEN annee_modele IS NOT NULL 
            THEN YEAR(CURRENT_DATE()) - annee_modele
            ELSE NULL
        END as age_vehicule
        
    FROM {{ ref('avitocardsataset') }}
    WHERE marque IS NOT NULL
),

vehicules_dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY marque, modele, annee_modele, type_carburant 
            ORDER BY boite_vitesses DESC
        ) as rn
    FROM vehicules_base
)

SELECT
    MD5(CONCAT(COALESCE(marque, ''), COALESCE(modele, ''), COALESCE(annee_modele, ''), COALESCE(type_carburant, ''))) as vehicule_id,
    vehicule_key,
    marque,
    modele,
    annee_modele,
    type_carburant,
    boite_vitesses,
    puissance_fiscale,
    is_premiere_main,
    nombre_portes,
    categorie_prix,
    age_vehicule
    
FROM vehicules_dedup
WHERE rn = 1