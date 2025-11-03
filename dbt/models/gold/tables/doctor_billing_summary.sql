{{ config(materialized='table') }}

WITH billing AS (
    SELECT
        patient_sk,
        hospital,
        SUM(ROUND(billing_amount, 2)) AS total_billed
    FROM {{ ref('vw_patient_summary') }}
    GROUP BY patient_sk, hospital
),

admissions AS (
    SELECT
        doctor_sk,
        patient_sk,
        hospital,
        SUM(total_admissions) AS total_admissions
    FROM {{ ref('fact_admissions') }}
    GROUP BY doctor_sk, patient_sk, hospital
)

SELECT
    doc.doctor_sk,
    doc.doctor,
    doc.hospital,
    COUNT(DISTINCT a.patient_sk) AS total_patients,
    SUM(a.total_admissions) AS total_admissions,
    ROUND(SUM(b.total_billed), 2) AS total_billed,
    
    -- ✅ Fixed division: use CASE instead of DIVIDE()
    ROUND(
        CASE 
            WHEN SUM(a.total_admissions) = 0 THEN 0
            ELSE SUM(b.total_billed) / SUM(a.total_admissions)
        END, 2
    ) AS avg_billed_per_admission

FROM admissions a
LEFT JOIN billing b
    ON a.patient_sk = b.patient_sk AND a.hospital = b.hospital
LEFT JOIN {{ ref('dim_doctor') }} doc
    ON a.doctor_sk = doc.doctor_sk
GROUP BY 1,2,3
