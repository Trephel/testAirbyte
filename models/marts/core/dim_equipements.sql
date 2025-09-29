{{
    config(
        materialized='table',
        alias='dim_equipements'
    )
}}

WITH equipements_base AS (
    SELECT 
        _airbyte_emitted_at as equipement_key,
        abs,
        airbags,
        climatisation,
        toit_ouvrant,
        radar_recul,
        camera_recul,
        cd_mp3_bluetooth,
        jantes_aluminium,
        ordinateur_bord,
        limiteur_vitesse,
        vitres_electriques,
        regulateur_vitesse,
        gps,
        sieges_cuir,
        esp,
        verrouillage_centralise,
        -- Score d'équipement
        (CASE WHEN abs THEN 1 ELSE 0 END +
         CASE WHEN airbags THEN 1 ELSE 0 END +
         CASE WHEN climatisation THEN 1 ELSE 0 END +
         CASE WHEN gps THEN 1 ELSE 0 END +
         CASE WHEN sieges_cuir THEN 1 ELSE 0 END) as score_equipement,
        -- Catégorie équipement
        CASE 
            WHEN (abs AND airbags AND climatisation AND gps AND sieges_cuir) THEN 'Premium'
            WHEN (abs AND airbags AND climatisation) THEN 'Confort'
            ELSE 'Basique'
        END as categorie_equipement
        
    FROM {{ ref('avitocardsataset') }}
)

SELECT
    MD5(CONCAT(
        COALESCE(CAST(abs AS VARCHAR), ''),
        COALESCE(CAST(airbags AS VARCHAR), ''),
        COALESCE(CAST(climatisation AS VARCHAR), '')
    )) as equipement_id,
    equipement_key,
    abs,
    airbags,
    climatisation,
    toit_ouvrant,
    radar_recul,
    camera_recul,
    cd_mp3_bluetooth,
    jantes_aluminium,
    ordinateur_bord,
    limiteur_vitesse,
    vitres_electriques,
    regulateur_vitesse,
    gps,
    sieges_cuir,
    esp,
    verrouillage_centralise,
    score_equipement,
    categorie_equipement
    
FROM equipements_base