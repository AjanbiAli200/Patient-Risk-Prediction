{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        name AS patient_name,
        gender,
        blood_type,
        medical_condition,
        insurance_provider
    FROM {{ source("source_silver","healthcare") }}
)
SELECT
    row_number() OVER (ORDER BY md5(patient_name)) AS patient_sk,
    patient_name,
    gender,
    blood_type,
    medical_condition,
    insurance_provider
FROM base;
