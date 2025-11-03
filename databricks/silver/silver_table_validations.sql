USE patient_risk_prediction.silver;

show tables;

SELECT DISTINCT hospital
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
