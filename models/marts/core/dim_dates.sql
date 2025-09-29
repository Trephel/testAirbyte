{{
    config(
        materialized='table',
        alias='dim_dates'
    )
}}

WITH date_range AS (
    SELECT 
        DATEADD(day, SEQ4(), '2020-01-01') as date_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365*10))  -- 10 ans de dates
),

dates_enriched AS (
    SELECT
        date_date,
        -- Clé date
        TO_CHAR(date_date, 'YYYYMMDD') as date_id,
        -- Composantes date
        YEAR(date_date) as annee,
        MONTH(date_date) as mois,
        DAY(date_date) as jour,
        QUARTER(date_date) as trimestre,
        -- Semaine
        WEEK(date_date) as semaine_annee,
        DAYOFWEEK(date_date) as jour_semaine,
        -- Noms
        DAYNAME(date_date) as nom_jour,
        MONTHNAME(date_date) as nom_mois,
        -- Flags
        CASE WHEN DAYOFWEEK(date_date) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
        CASE WHEN date_date = CURRENT_DATE() THEN TRUE ELSE FALSE END as is_aujourdhui,
        CASE WHEN date_date < CURRENT_DATE() THEN TRUE ELSE FALSE END as is_passe,
        -- Mois/Année format
        TO_CHAR(date_date, 'YYYY-MM') as annee_mois,
        -- Date complète format français
        TO_CHAR(date_date, 'DD/MM/YYYY') as date_fr
        
    FROM date_range
)

SELECT
    date_id,
    date_date,
    annee,
    mois,
    jour,
    trimestre,
    semaine_annee,
    jour_semaine,
    nom_jour,
    nom_mois,
    is_weekend,
    is_aujourdhui,
    is_passe,
    annee_mois,
    date_fr
    
FROM dates_enriched
WHERE date_date <= CURRENT_DATE()