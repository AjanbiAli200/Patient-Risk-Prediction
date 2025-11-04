{{ config(
    materialized='table',
    schema='ml'
) }}

SELECT
    name AS patient_name,
    gender,
    age,
    blood_type,
    medical_condition,
    hospital,
    insurance_provider,
    billing_amount,
    medication,
    test_results,

    -- Derive risk level (based on condition and stay)
    CASE 
        WHEN medical_condition IN ('Cancer', 'Heart Disease', 'Diabetes', 'Hypertension')
             OR stay_duration_days > 15 THEN 'High'
        ELSE 'Normal'
    END AS risk_level,

    stay_duration_days AS target_stay_duration

FROM {{ source('source_silver', 'healthcare') }}
WHERE stay_duration_days IS NOT NULL
