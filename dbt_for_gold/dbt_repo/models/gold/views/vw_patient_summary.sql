{{ config(materialized='view') }}

SELECT
    p.patient_sk,
    h.name AS patient_name,
    h.age,
    h.gender,
    h.medical_condition,
    h.hospital,
    h.insurance_provider,
    h.medication,
    h.admission_type,
    h.date_of_admission,
    h.discharge_date,
    h.stay_duration_days,
    ROUND(h.billing_amount, 2) AS billing_amount,
    h.test_results
FROM {{ source("source_silver", "healthcare") }} AS h
LEFT JOIN {{ ref('dim_patient') }} AS p
    ON h.name = p.patient_name
    AND h.gender = p.gender
    AND h.medical_condition = p.medical_condition
    AND h.insurance_provider = p.insurance_provider
ORDER BY h.date_of_admission DESC;
