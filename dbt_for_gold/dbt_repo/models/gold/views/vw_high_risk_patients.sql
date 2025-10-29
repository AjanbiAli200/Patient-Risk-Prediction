{{ config(materialized='view') }}

SELECT
    p.patient_sk,
    h.name AS patient_name,
    h.age,
    h.gender,
    h.medical_condition,
    h.hospital,
    h.insurance_provider,
    ROUND(h.billing_amount, 2) AS billing_amount,
    h.stay_duration_days,
    CASE 
        WHEN h.medical_condition IN ('Cancer', 'Heart Disease', 'Diabetes', 'Hypertension')
             OR h.stay_duration_days > 15
        THEN 'High'
        ELSE 'Normal'
    END AS risk_level
FROM {{ source("source_silver", "healthcare") }} h
LEFT JOIN {{ ref('dim_patient') }} p
    ON h.name = p.patient_name
    AND h.gender = p.gender
    AND h.medical_condition = p.medical_condition
    AND h.insurance_provider = p.insurance_provider;
