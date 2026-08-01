{# dbt model template #}
{# This is a staging model for {dataset_name} #}

{{ config(
    materialized='table',
    schema='staging'
) }}

-- Staging model for {dataset_name} data

WITH source AS (
    SELECT *
    FROM {{ source('raw', '{dataset_name}_source') }}
),

renamed AS (
    SELECT
        -- Add your field mappings here
        -- Example:
        -- id as user_id,
        -- name as user_name,
        -- created_at as user_created_at,
        *
    FROM source
)

SELECT * FROM renamed