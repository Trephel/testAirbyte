{{
    config(
        materialized='table',
        alias='fact_annonces'
    )
}}

WITH annonces AS (
    SELECT 
        a._airbyte_ab_id as annonce_id,
        a._airbyte_emitted_at as annonce_key,
        a._airbyte_emitted_at as date_publication,
        a.prix_euros,
        a.kilometrage,
        
        -- Clés étrangères
        MD5(CONCAT(COALESCE(a.marque, ''), COALESCE(a.modele, ''), COALESCE(a.annee_modele, ''), COALESCE(a.type_carburant_clean, ''))) as vehicule_id,
        MD5(CONCAT(COALESCE(a.ville, ''), COALESCE(a.secteur, ''))) as geographie_id,
        MD5(CONCAT(
            COALESCE(CAST(a.abs AS VARCHAR), ''),
            COALESCE(CAST(a.airbags AS VARCHAR), ''),
            COALESCE(CAST(a.climatisation AS VARCHAR), '')
        )) as equipement_id,
        TO_CHAR(a._airbyte_emitted_at, 'YYYYMMDD') as date_id,  -- Clé vers dim_dates
        
        -- Métriques qualité
        a.missing_prix_flag,
        a.missing_marque_flag,
        a.missing_annee_flag,
        
        -- Prix au km (indicateur)
        CASE 
            WHEN a.kilometrage > 0 AND a.prix_euros > 0 
            THEN ROUND(a.prix_euros / a.kilometrage, 4)
            ELSE NULL
        END as prix_par_km
        
    FROM {{ ref('avitocardsataset') }} a
    WHERE a.prix_euros IS NOT NULL
)

SELECT
    annonce_id,
    annonce_key,
    date_publication,
    date_id,  -- Nouvelle colonne
    vehicule_id,
    geographie_id,
    equipement_id,
    prix_euros,
    kilometrage,
    prix_par_km,
    missing_prix_flag,
    missing_marque_flag,
    missing_annee_flag
    
FROM annonces