{{ config(materialized='table') }}

WITH base AS (
    SELECT
        doctor,
        hospital,
        admission_type,
        name AS patient_name,
        gender,
        blood_type,
        medical_condition,
        insurance_provider,
        COUNT(*) AS total_admissions,
        ROUND(AVG(stay_duration_days), 2) AS avg_stay_duration
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY doctor, hospital, admission_type, name, gender, blood_type, medical_condition, insurance_provider
)

SELECT
    -- Stable surrogate key using md5 hash of concatenated fields
    row_number() OVER (
        ORDER BY md5(
            COALESCE(b.doctor,'') || COALESCE(b.hospital,'') || COALESCE(b.admission_type,'') || COALESCE(b.patient_name,'')
        )
    ) AS admission_sk,

    -- Foreign keys
    d.doctor_sk,
    p.patient_sk,

    -- Original attributes
    b.doctor,
    b.hospital,
    b.admission_type,
    b.total_admissions,
    b.avg_stay_duration

FROM base b

-- Join to dim_doctor
LEFT JOIN {{ ref('dim_doctor') }} d
    ON COALESCE(b.doctor,'') = COALESCE(d.doctor,'')
    AND COALESCE(b.hospital,'') = COALESCE(d.hospital,'')

-- Join to dim_patient
LEFT JOIN {{ ref('dim_patient') }} p
    ON COALESCE(b.patient_name,'') = COALESCE(p.patient_name,'')
    AND COALESCE(b.gender,'') = COALESCE(p.gender,'')
    AND COALESCE(b.medical_condition,'') = COALESCE(p.medical_condition,'')
    AND COALESCE(b.insurance_provider,'') = COALESCE(p.insurance_provider,'');
