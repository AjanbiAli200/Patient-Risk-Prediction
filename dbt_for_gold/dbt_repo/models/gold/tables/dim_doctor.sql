{{ config(materialized='table') }}

WITH base AS (
    SELECT
        doctor,
        hospital,
        COUNT(DISTINCT medical_condition) AS conditions_treated,
        COUNT(*) AS total_patients
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY 1,2
)
SELECT
    row_number() OVER (ORDER BY md5(concat(doctor, hospital))) AS doctor_sk,
    doctor,
    hospital,
    conditions_treated,
    total_patients
FROM base;
