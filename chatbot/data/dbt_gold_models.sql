-- dim_date.sql

{{ config(materialized='table') }}

-- Generate a date dimension covering the full range of your dataset
WITH date_range AS (
    SELECT
        sequence(
            to_date('2010-01-01'),
            to_date('2030-12-31'),
            interval 1 day
        ) AS date_seq
),

exploded AS (
    SELECT explode(date_seq) AS full_date FROM date_range
),

final AS (
    SELECT
        full_date,
        year(full_date) AS year,
        quarter(full_date) AS quarter,
        month(full_date) AS month,
        day(full_date) AS day,
        weekofyear(full_date) AS week_of_year,
        date_format(full_date, 'MMMM') AS month_name,
        date_format(full_date, 'EEEE') AS day_name,
        CASE 
            WHEN dayofweek(full_date) IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        concat('Q', quarter(full_date)) AS quarter_name,
        make_date(year(full_date), month(full_date), 1) AS month_start,
        last_day(full_date) AS month_end
    FROM exploded
)

SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_sk,  -- ✅ Sequential surrogate key
    *
FROM final
ORDER BY full_date;


-- dim_doctor.sql

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        doctor,
        hospital,
        COUNT(DISTINCT medical_condition) AS conditions_treated,
        COUNT(*) AS total_patients
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY 1,2
)
SELECT
    row_number() OVER (ORDER BY md5(concat(doctor, hospital))) AS doctor_sk,
    doctor,
    hospital,
    conditions_treated,
    total_patients
FROM base;


-- dim_hospital.sql

{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        TRIM(hospital) AS hospital,
        TRIM(REGEXP_SUBSTR(hospital, '^[^,\\-]+')) AS parent_hospital
    FROM {{ source("source_silver", "healthcare") }}
    WHERE hospital IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY parent_hospital, hospital) AS hospital_sk,
    parent_hospital,
    hospital
FROM base;


-- dim_patient.sql

{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        name AS patient_name,
        gender,
        blood_type,
        medical_condition,
        insurance_provider
    FROM {{ source("source_silver","healthcare") }}
)
SELECT
    row_number() OVER (ORDER BY md5(patient_name)) AS patient_sk,
    patient_name,
    gender,
    blood_type,
    medical_condition,
    insurance_provider
FROM base;


-- doctor_billing_summary.sql

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

-- fact_admissions.sql

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


-- fact_billing_summary.sql

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


