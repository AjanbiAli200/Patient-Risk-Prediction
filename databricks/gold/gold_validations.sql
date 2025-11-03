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