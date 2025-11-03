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
        ROUND(SUM(billing_amount), 2) AS total_billed,
        ROUND(AVG(billing_amount), 2) AS avg_bill,
        COUNT(DISTINCT name) AS total_patients
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY doctor, hospital, admission_type, name, gender, blood_type, 
             medical_condition, insurance_provider, date_of_admission, discharge_date
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY md5(
            COALESCE(b.doctor,'') || COALESCE(b.hospital,'') || 
            COALESCE(b.patient_name,'') || COALESCE(b.admission_type,'')
        )
    ) AS billing_sk,

    -- Foreign keys
    d.doctor_sk,
    p.patient_sk,
    h.hospital_sk,
    d_bill.date_sk AS billing_date_sk,

    -- Original attributes
    b.doctor,
    b.hospital,
    b.admission_type,
    b.total_billed,
    b.avg_bill,
    b.total_patients,
    b.date_of_admission AS billing_date

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
LEFT JOIN {{ ref('dim_date') }} d_bill
    ON to_date(b.date_of_admission) = d_bill.full_date;
