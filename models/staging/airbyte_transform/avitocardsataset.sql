{{
    config(
        materialized='table',
        alias='avitocardsataset'
    )
}}

WITH extracted AS (
    SELECT
        _airbyte_ab_id,
        
        _airbyte_emitted_at,
       
        
        -- Extraction des colonnes principales directement depuis la source
        _airbyte_data:"Unnamed: 0"::VARCHAR as unnamed_index,
        _airbyte_data:"LIEN"::VARCHAR as lien_annonce,
        _airbyte_data:"PRIX"::VARCHAR as prix_texte,
        _airbyte_data:"VILLE"::VARCHAR as ville,
        _airbyte_data:"SECTEUR"::VARCHAR as secteur,
        _airbyte_data:"ORIGINE"::VARCHAR as origine,
        _airbyte_data:"MARQUE"::VARCHAR as marque,
        _airbyte_data:"Modèle"::VARCHAR as modele,
        _airbyte_data:"Année-Modèle"::VARCHAR as annee_modele_texte,
        _airbyte_data:"Kilométrage"::VARCHAR as kilometrage_texte,
        _airbyte_data:"Type de carburant"::VARCHAR as type_carburant,
        _airbyte_data:"Boite de vitesses"::VARCHAR as boite_vitesses,
        _airbyte_data:"Puissance fiscale"::VARCHAR as puissance_fiscale,
        
        -- Extraction des équipements (booléens)
        _airbyte_data:"ABS"::BOOLEAN as abs,
        _airbyte_data:"AIRBAGS"::BOOLEAN as airbags,
        _airbyte_data:"CLIMATISATION"::BOOLEAN as climatisation,
        _airbyte_data:"Toit ouvrant"::BOOLEAN as toit_ouvrant,
        _airbyte_data:"Première main"::VARCHAR as premiere_main,
        _airbyte_data:"Radar de recul"::BOOLEAN as radar_recul,
        _airbyte_data:"Caméra de recul"::BOOLEAN as camera_recul,
        _airbyte_data:"CD/MP3/Bluetooth"::BOOLEAN as cd_mp3_bluetooth,
        _airbyte_data:"Jantes aluminium"::BOOLEAN as jantes_aluminium,
        _airbyte_data:"Nombre de portes"::VARCHAR as nombre_portes,
        _airbyte_data:"Ordinateur de bord"::BOOLEAN as ordinateur_bord,
        _airbyte_data:"Limiteur de vitesse"::BOOLEAN as limiteur_vitesse,
        _airbyte_data:"Vitres électriques"::BOOLEAN as vitres_electriques,
        _airbyte_data:"Régulateur de vitesse"::BOOLEAN as regulateur_vitesse,
        _airbyte_data:"Système de navigation/GPS"::BOOLEAN as gps,
        _airbyte_data:"Sièges cuir"::BOOLEAN as sieges_cuir,
        _airbyte_data:"ESP"::BOOLEAN as esp,
        _airbyte_data:"Verrouillage centralisé à distance"::BOOLEAN as verrouillage_centralise
        
    FROM {{ source('airbyte_raw', '_AIRBYTE_RAW_AVITOCARDATASET') }}
),

cleaned AS (
    SELECT
        _airbyte_ab_id,
      
        _airbyte_emitted_at,
       
        unnamed_index,
        lien_annonce,
        
        -- Nettoyage du prix (supprimer "€", espaces et convertir)
        CASE 
            WHEN prix_texte IS NOT NULL 
            THEN TRY_CAST(REPLACE(REPLACE(prix_texte, '€', ''), ' ', '') AS FLOAT)
            ELSE NULL 
        END as prix_euros,
        
        -- Nettoyage texte
        INITCAP(TRIM(ville)) as ville,
        UPPER(TRIM(secteur)) as secteur,
        INITCAP(TRIM(origine)) as origine,
        UPPER(TRIM(marque)) as marque,
        INITCAP(TRIM(modele)) as modele,
        
        -- Conversion année-modèle
        CASE 
            WHEN LENGTH(TRIM(annee_modele_texte)) = 4 
                 AND TRY_CAST(annee_modele_texte AS INT) IS NOT NULL
                 AND TRY_CAST(annee_modele_texte AS INT) BETWEEN 1900 AND 2024
            THEN TRY_CAST(annee_modele_texte AS INT)
            ELSE NULL 
        END as annee_modele,
        
        -- Conversion kilométrage (supprimer "km", espaces)
        CASE 
            WHEN kilometrage_texte IS NOT NULL 
            THEN TRY_CAST(REPLACE(REPLACE(REPLACE(kilometrage_texte, 'km', ''), ' ', ''), ',', '') AS INT)
            ELSE NULL 
        END as kilometrage,
        
        -- Standardisation type carburant
        CASE 
            WHEN UPPER(type_carburant) LIKE '%ESSENCE%' THEN 'Essence'
            WHEN UPPER(type_carburant) LIKE '%DIESEL%' THEN 'Diesel'
            WHEN UPPER(type_carburant) LIKE '%ELECTRI%' THEN 'Electrique'
            WHEN UPPER(type_carburant) LIKE '%HYBRID%' THEN 'Hybride'
            WHEN type_carburant IS NULL THEN 'Non spécifié'
            ELSE INITCAP(TRIM(type_carburant))
        END as type_carburant_clean,
        
        -- Standardisation boite de vitesses
        CASE 
            WHEN UPPER(boite_vitesses) LIKE '%AUTO%' THEN 'Automatique'
            WHEN UPPER(boite_vitesses) LIKE '%MANU%' THEN 'Manuelle'
            WHEN boite_vitesses IS NULL THEN 'Non spécifié'
            ELSE INITCAP(TRIM(boite_vitesses))
        END as boite_vitesses_clean,
        
        -- Nettoyage puissance fiscale
        CASE 
            WHEN LENGTH(TRIM(puissance_fiscale)) > 0 
                 AND TRY_CAST(puissance_fiscale AS INT) IS NOT NULL
            THEN TRY_CAST(puissance_fiscale AS INT)
            ELSE NULL 
        END as puissance_fiscale_clean,
        
        -- Standardisation première main
        CASE 
            WHEN UPPER(premiere_main) LIKE '%OUI%' THEN TRUE
            WHEN UPPER(premiere_main) LIKE '%NON%' THEN FALSE
            ELSE NULL 
        END as is_premiere_main,
        
        -- Conversion nombre de portes
        CASE 
            WHEN LENGTH(TRIM(nombre_portes)) > 0 
                 AND TRY_CAST(nombre_portes AS INT) IS NOT NULL
            THEN TRY_CAST(nombre_portes AS INT)
            ELSE NULL 
        END as nombre_portes_clean,
        
        -- Équipements (déjà en booléen)
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
        
        -- Flags de qualité données
        CASE WHEN prix_texte IS NULL OR TRIM(prix_texte) = '' THEN 1 ELSE 0 END as missing_prix_flag,
        CASE WHEN marque IS NULL OR TRIM(marque) = '' THEN 1 ELSE 0 END as missing_marque_flag,
        CASE WHEN annee_modele_texte IS NULL OR TRIM(annee_modele_texte) = '' THEN 1 ELSE 0 END as missing_annee_flag
        
    FROM extracted
)

SELECT * FROM cleaned