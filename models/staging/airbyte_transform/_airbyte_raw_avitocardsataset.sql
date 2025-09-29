{{
    config(
        materialized='ephemeral',
        alias='AVITOCARDATASET'
    )
}}

-- Ce modèle lit les données brutes Airbyte
SELECT 
    AIRBYTE_DATABASE.AIRBYTE_SCHEMA.AVITOCARDATASET._AIRBYTE_AB_ID,
    AIRBYTE_DATABASE.AIRBYTE_SCHEMA.AVITOCARDATASET._AIRBYTE_EMITTED_AT,
FROM {{ source('AIRBYTE_DATABASE', 'AVITOCARDATASET') }}