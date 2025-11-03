{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        p.patient_sk,
        h.name AS patient_name,
        h.age,
        CASE 
            WHEN h.age < 18 THEN 'Under 18'
            WHEN h.age BETWEEN 18 AND 29 THEN '18–29'
            WHEN h.age BETWEEN 30 AND 44 THEN '30–44'
            WHEN h.age BETWEEN 45 AND 59 THEN '45–59'
            WHEN h.age BETWEEN 60 AND 74 THEN '60–74'
            ELSE '75+'
        END AS age_group,
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
        h.test_results,

        -- ✅ Added comma before ROW_NUMBER()
        CASE 
            WHEN h.medical_condition IN ('Cancer', 'Heart Disease', 'Diabetes', 'Hypertension')
                OR h.stay_duration_days > 15
            THEN 'High'
            ELSE 'Normal'
        END AS risk_level,

        ROW_NUMBER() OVER (
            PARTITION BY p.patient_sk, h.date_of_admission
            ORDER BY h.discharge_date DESC
        ) AS rn
    FROM {{ source("source_silver", "healthcare") }} h
    LEFT JOIN {{ ref('dim_patient') }} p
        ON h.name = p.patient_name
        AND h.gender = p.gender
        AND h.medical_condition = p.medical_condition
        AND h.insurance_provider = p.insurance_provider
)

SELECT
    r.*,
    d_admit.date_sk AS admission_date_sk,
    d_discharge.date_sk AS discharge_date_sk
FROM ranked r
LEFT JOIN {{ ref('dim_date') }} d_admit
    ON TO_DATE(r.date_of_admission) = d_admit.full_date
LEFT JOIN {{ ref('dim_date') }} d_discharge
    ON TO_DATE(r.discharge_date) = d_discharge.full_date
WHERE r.rn = 1
ORDER BY r.date_of_admission DESC;
