-- silver_table_validations.sql

USE patient_risk_prediction.silver;

show tables;

SELECT *
FROM patient_risk_prediction.silver.healthcare
LIMIT 50;


select distinct(medical_condition) from patient_risk_prediction.silver.healthcare;

SELECT 
    hospital,
    COUNT(*) AS hospital_record_count
FROM patient_risk_prediction.silver.healthcare
GROUP BY hospital
ORDER BY hospital
LIMIT 30;

SELECT
  name,
  COUNT(*) AS records,
  COUNT(DISTINCT date_of_admission) AS distinct_dates
FROM patient_risk_prediction.silver.healthcare
GROUP BY name
HAVING COUNT(*) > 1
LIMIT 10;


-- gold_objects_validation.sql  

USE patient_risk_prediction.gold;

SHOW TABLES;

select * from patient_risk_prediction.gold.dim_doctor;

select * from patient_risk_prediction.gold.dim_patient
limit 10;


DESCRIBE patient_risk_prediction.gold.dim_patient;

select count(*) from patient_risk_prediction.gold.dim_doctor; --50000

select * from patient_risk_prediction.gold.dim_doctor
limit 10;

select doctor_sk,count(*) from patient_risk_prediction.gold.dim_doctor
group by doctor_sk
having count(*) > 1
ORDER BY doctor_sk ASC
limit 10;     --None

select count(*) from patient_risk_prediction.gold.dim_patient; --49952

select * from patient_risk_prediction.gold.dim_patient
limit 10;

select patient_sk,count(*) from patient_risk_prediction.gold.dim_patient
group by patient_sk
having count(*) > 1
ORDER BY patient_sk ASC
limit 10;   --None

select count(*) from patient_risk_prediction.gold.fact_admissions;  --50539

select * from patient_risk_prediction.gold.fact_admissions
limit 10;

select patient_sk,count(*) from patient_risk_prediction.gold.fact_admissions
group by patient_sk
having count(*) > 1
ORDER BY patient_sk ASC
limit 10;   --have

select * from patient_risk_prediction.gold.vw_patient_summary
where patient_sk = 7;


select count(*) from patient_risk_prediction.gold.fact_billing_summary;  --45780

select * from patient_risk_prediction.gold.fact_billing_summary
limit 10;

select billing_sk,count(*) from patient_risk_prediction.gold.fact_billing_summary
group by billing_sk
having count(*) > 1
ORDER BY billing_sk ASC
limit 10;   --None

select count(*) from patient_risk_prediction.gold.vw_avg_stay_by_hospital;  --39876

select * from patient_risk_prediction.gold.vw_avg_stay_by_hospital
limit 10;

select hospital_sk,count(*) from patient_risk_prediction.gold.vw_avg_stay_by_hospital
group by hospital_sk
having count(*) > 1
ORDER BY hospital_sk ASC
limit 10;  --None

select count(*) from patient_risk_prediction.gold.vw_high_risk_patients;   --56097

select patient_sk,count(*) from patient_risk_prediction.gold.vw_high_risk_patients
group by patient_sk
having count(*) > 1
ORDER BY patient_sk ASC
limit 10;  --have

select * from patient_risk_prediction.gold.vw_high_risk_patients
where patient_sk = 7;


select * from patient_risk_prediction.gold.vw_high_risk_patients
limit 10;

select count(*) from patient_risk_prediction.gold.vw_patient_summary;   --56097

select patient_sk,count(*) from patient_risk_prediction.gold.vw_patient_summary
group by patient_sk
having count(*) > 1
ORDER BY patient_sk ASC
limit 10;   --Have

select * from patient_risk_prediction.gold.vw_patient_summary
limit 10;
--where patient_sk = 7;

select count(*) from patient_risk_prediction.gold.vw_total_billing_by_insurance; --5

select * from patient_risk_prediction.gold.vw_total_billing_by_insurance
limit 10;

select insurance_sk,count(*) from patient_risk_prediction.gold.vw_total_billing_by_insurance
group by insurance_sk
having count(*) > 1
ORDER BY insurance_sk ASC
limit 10;   --None

SELECT 
    hospital,
    COUNT(*) AS hospital_record_count
FROM patient_risk_prediction.gold.dim_hospital
GROUP BY hospital
ORDER BY hospital
LIMIT 0;
 
select count(*) from patient_risk_prediction.gold.dim_doctor;

SELECT COUNT(DISTINCT parent_hospital) AS total_distinct_hospitals
FROM patient_risk_prediction.gold.dim_hospital;


SELECT COUNT(DISTINCT hospital) AS total_distinct_hospitals
FROM patient_risk_prediction.silver.healthcare;

select * from patient_risk_prediction.gold.doctor_billing_summary;

select 
doctor,
total_patients,
count(*)
from patient_risk_prediction.gold.doctor_billing_summary
group by 1,2
limit 50;

SELECT 
  doctor,
  SUM(total_patients) AS total_patients_handled
FROM patient_risk_prediction.gold.doctor_billing_summary
GROUP BY doctor
ORDER BY total_patients_handled DESC
limit 50;

select * from patient_risk_prediction.gold.dim_date
limit 10;


-- Final_Queries.sql

--🧩 1️⃣ Age Group vs Disease Type

--Goal: Find number of patients per disease type and age group.
--(You can later visualize this in Power BI as a clustered bar or heat map.)

SELECT 
    p.age_group,
    p.medical_condition AS disease_type,
    COUNT(*) AS total_patients,
    SUM(CASE WHEN h.risk_level = 'High' THEN 1 ELSE 0 END) AS high_risk_patients,
    ROUND(100.0 * SUM(CASE WHEN h.risk_level = 'High' THEN 1 ELSE 0 END) / COUNT(*), 2) AS high_risk_percent
FROM patient_risk_prediction.gold.vw_patient_summary AS p
LEFT JOIN patient_risk_prediction.gold.vw_high_risk_patients AS h 
    ON p.patient_sk = h.patient_sk
GROUP BY 
    p.age_group,
    p.medical_condition
ORDER BY 
    p.age_group, total_patients DESC;



--💊 2. Treatment Effectiveness (Based on Medication & Risk Level)
--You don’t have a treatment type, but you do have medication and risk_level.
--We can use those to measure effectiveness by assuming “Normal” test results = positive outcome.
use patient_risk_prediction.gold;

SELECT
    medical_condition,
    medication,
    COUNT(*) AS total_patients,
    SUM(CASE WHEN test_results = 'Normal' THEN 1 ELSE 0 END) AS recovered_count,
    ROUND(100.0 * SUM(CASE WHEN test_results = 'Normal' THEN 1 ELSE 0 END) / COUNT(*), 2) AS effectiveness_rate,
    ROUND(AVG(stay_duration_days), 1) AS avg_stay_days,
    ROUND(AVG(billing_amount), 2) AS avg_cost
FROM patient_risk_prediction.gold.vw_patient_summary
GROUP BY
    medication,
    medical_condition
ORDER BY
    effectiveness_rate DESC;


--🔁 3. Readmission Analysis (Patients readmitted multiple times)
--You can detect readmissions by counting how often each patient appears.

SELECT
    patient_name,
    medical_condition,
    COUNT(*) AS total_admissions,
    MIN(date_of_admission) AS first_admission,
    MAX(discharge_date) AS last_discharge,
    DATEDIFF(MAX(discharge_date), MIN(date_of_admission)) AS total_duration_span,
    ROUND(AVG(stay_duration_days), 1) AS avg_stay_per_admission,
    ROUND(AVG(billing_amount), 2) AS avg_billing_per_stay
FROM patient_risk_prediction.gold.vw_patient_summary
GROUP BY
    patient_name,
    medical_condition
HAVING
    COUNT(*) > 1
ORDER BY
    total_admissions DESC;
