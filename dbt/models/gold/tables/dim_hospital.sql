{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        TRIM(hospital) AS hospital,
        TRIM(REGEXP_SUBSTR(hospital, '^[^,\\-]+')) AS parent_hospital
    FROM {{ source("source_silver", "healthcare") }}
    WHERE hospital IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY parent_hospital, hospital) AS hospital_sk,
    parent_hospital,
    hospital
FROM base;
