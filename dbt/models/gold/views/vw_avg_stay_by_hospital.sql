{{ config(materialized='view') }}

SELECT
    h.hospital_sk,
    s.hospital,
    COUNT(*) AS total_patients,
    ROUND(AVG(s.stay_duration_days), 2) AS avg_stay_days,
    MIN(s.stay_duration_days) AS min_stay_days,
    MAX(s.stay_duration_days) AS max_stay_days
FROM {{ source("source_silver", "healthcare") }} s
LEFT JOIN {{ ref('dim_hospital') }} h
    ON COALESCE(s.hospital,'') = COALESCE(h.hospital,'')
GROUP BY h.hospital_sk, s.hospital
ORDER BY avg_stay_days DESC;
