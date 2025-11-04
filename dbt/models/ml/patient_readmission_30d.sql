{{ config(
    materialized='table',
    schema='ml'
) }}

WITH ordered AS (
    SELECT
        name AS patient_name,
        gender,
        age,
        medical_condition,
        hospital,
        insurance_provider,
        date_of_admission,
        discharge_date,
        stay_duration_days,
        billing_amount,

        ROW_NUMBER() OVER (
            PARTITION BY name
            ORDER BY date_of_admission
        ) AS encounter_id,

        LEAD(date_of_admission) OVER (
            PARTITION BY name
            ORDER BY date_of_admission
        ) AS next_admission
    FROM {{ source('source_silver', 'healthcare') }}
    WHERE discharge_date IS NOT NULL
),
flagged AS (
    SELECT
        *,
        DATEDIFF(next_admission, discharge_date) AS days_to_next_admit,
        CASE
            WHEN next_admission IS NOT NULL
                 AND next_admission > discharge_date
                 AND DATEDIFF(next_admission, discharge_date) BETWEEN 0 AND 30 THEN 1
            ELSE 0
        END AS readmitted_within_30d
    FROM ordered
)
SELECT
    patient_name,
    gender,
    age,
    medical_condition,
    hospital,
    insurance_provider,
    stay_duration_days,
    billing_amount,
    days_to_next_admit,
    readmitted_within_30d,
    CASE 
        WHEN medical_condition IN ('Cancer','Heart Disease','Diabetes','Hypertension')
             OR stay_duration_days > 15 THEN 'High'
        ELSE 'Normal'
    END AS risk_level
FROM flagged
