{{ config(materialized='view') }}

SELECT
    row_number() OVER (ORDER BY md5(hospital)) AS hospital_sk,
    hospital,
    COUNT(*) AS total_patients,
    ROUND(AVG(stay_duration_days), 2) AS avg_stay_days,
    MIN(stay_duration_days) AS min_stay_days,
    MAX(stay_duration_days) AS max_stay_days
FROM {{ source("source_silver", "healthcare") }}
GROUP BY hospital
ORDER BY avg_stay_days DESC;
