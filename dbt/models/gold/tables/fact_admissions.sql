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
        date_of_admission,
        discharge_date,
        COUNT(*) AS total_admissions,
        ROUND(AVG(stay_duration_days), 2) AS avg_stay_duration
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY doctor, hospital, admission_type, name, gender, blood_type, 
             medical_condition, insurance_provider, date_of_admission, discharge_date
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY md5(
            COALESCE(b.doctor,'') || COALESCE(b.hospital,'') || 
            COALESCE(b.admission_type,'') || COALESCE(b.patient_name,'')
        )
    ) AS admission_sk,

    -- Foreign keys
    d.doctor_sk,
    p.patient_sk,
    h.hospital_sk,
    d_admit.date_sk AS admission_date_sk,
    d_discharge.date_sk AS discharge_date_sk,

    -- Original attributes
    b.doctor,
    b.hospital,
    b.admission_type,
    b.total_admissions,
    b.avg_stay_duration,
    b.date_of_admission,
    b.discharge_date

FROM base b
LEFT JOIN {{ ref('dim_doctor') }} d
    ON b.doctor = d.doctor AND b.hospital = d.hospital
LEFT JOIN {{ ref('dim_patient') }} p
    ON b.patient_name = p.patient_name 
    AND b.gender = p.gender 
    AND b.medical_condition = p.medical_condition 
    AND b.insurance_provider = p.insurance_provider
LEFT JOIN {{ ref('dim_hospital') }} h
    ON b.hospital = h.hospital
LEFT JOIN {{ ref('dim_date') }} d_admit
    ON to_date(b.date_of_admission) = d_admit.full_date
LEFT JOIN {{ ref('dim_date') }} d_discharge
    ON to_date(b.discharge_date) = d_discharge.full_date;
