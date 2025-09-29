{{
    config(
        materialized='table',
        alias='fct_analyse_vehicules'
    )
}}

SELECT
    f.annonce_id,
    f.date_publication,
    
    -- Dimensions véhicule
    v.marque,
    v.modele,
    v.annee_modele,
    v.type_carburant,
    v.categorie_prix,
    v.age_vehicule,
    
    -- Dimensions géographie
    g.ville,
    g.secteur,
    g.region,
    
    -- Dimensions équipements
    e.score_equipement,
    e.categorie_equipement,
    
    -- Faits
    f.prix_euros,
    f.kilometrage,
    f.prix_par_km,
    
    -- Agrégations
    AVG(f.prix_euros) OVER (PARTITION BY v.marque, v.modele) as prix_moyen_marque_modele,
    COUNT(f.annonce_id) OVER (PARTITION BY g.ville) as nb_annonces_ville,
    
    -- Flags
    f.missing_prix_flag,
    f.missing_marque_flag
    
FROM {{ ref('fact_annonces') }} f
LEFT JOIN {{ ref('dim_vehicules') }} v ON f.vehicule_id = v.vehicule_id
LEFT JOIN {{ ref('dim_geographie') }} g ON f.geographie_id = g.geographie_id
LEFT JOIN {{ ref('dim_equipements') }} e ON f.equipement_id = e.equipement_id