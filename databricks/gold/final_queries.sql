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


age_group	disease_type	total_patients	high_risk_patients	high_risk_percent
18–29	Obesity	1541	728	47.24
18–29	Diabetes	1510	1510	100.00
18–29	Cancer	1508	1508	100.00
18–29	Hypertension	1495	1495	100.00
18–29	Asthma	1492	759	50.87
18–29	Arthritis	1486	729	49.06
30–44	Arthritis	1987	980	49.32
30–44	Diabetes	1905	1905	100.00
30–44	Hypertension	1892	1892	100.00
30–44	Obesity	1881	921	48.96
30–44	Asthma	1869	946	50.62
30–44	Cancer	1868	1868	100.00
45–59	Diabetes	1973	1973	100.00
45–59	Obesity	1951	994	50.95
45–59	Arthritis	1943	987	50.80
45–59	Hypertension	1909	1909	100.00
45–59	Cancer	1899	1899	100.00
45–59	Asthma	1864	977	52.41
60–74	Diabetes	1925	1925	100.00
60–74	Cancer	1903	1903	100.00
60–74	Asthma	1896	964	50.84
60–74	Hypertension	1886	1886	100.00
60–74	Arthritis	1872	920	49.15
60–74	Obesity	1866	928	49.73
75+	Arthritis	1460	764	52.33
75+	Asthma	1417	718	50.67
75+	Hypertension	1391	1391	100.00
75+	Cancer	1390	1390	100.00
75+	Diabetes	1379	1379	100.00
75+	Obesity	1371	704	51.35


--💊 2. Treatment Effectiveness (Based on Medication & Risk Level)
--You don’t have a treatment type, but you do have medication and risk_level.
--We can use those to measure effectiveness by assuming “Normal” test results = positive outcome.

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

medical_condition	medication	total_patients	recovered_count	effectiveness_rate	avg_stay_days	avg_cost
Asthma	Ibuprofen	1638	577	35.23	15.8	26052.53
Hypertension	Aspirin	1702	597	35.08	15.4	25438.39
Asthma	Penicillin	1697	591	34.83	15.4	25701.61
Asthma	Aspirin	1633	568	34.78	15.7	25732.18
Hypertension	Ibuprofen	1711	590	34.48	15.1	25833.09
Hypertension	Penicillin	1624	559	34.42	15.8	25433.95
Diabetes	Penicillin	1713	587	34.27	15.5	25703.29
Asthma	Paracetamol	1722	588	34.15	15.3	25797.92
Obesity	Aspirin	1683	573	34.05	15.3	26113.29
Arthritis	Lipitor	1689	571	33.81	15.5	25269.53
Diabetes	Ibuprofen	1698	574	33.80	15.3	25326.69
Cancer	Aspirin	1619	545	33.66	15.5	25448.65
Asthma	Lipitor	1666	559	33.55	16.1	25118.78
Obesity	Paracetamol	1634	548	33.54	15.8	25671.57
Hypertension	Paracetamol	1687	565	33.49	15.4	25096.21
Cancer	Lipitor	1741	581	33.37	15.1	25169.4
Cancer	Ibuprofen	1701	567	33.33	15.6	25341.51
Diabetes	Aspirin	1698	564	33.22	15.3	25431.25
Arthritis	Paracetamol	1710	566	33.10	15.5	25303.73
Obesity	Penicillin	1737	575	33.10	15.4	25711.14
Diabetes	Lipitor	1722	567	32.93	15.3	25456.22
Cancer	Paracetamol	1688	555	32.88	15.5	25349.46
Obesity	Lipitor	1652	543	32.87	15.7	25283.17
Arthritis	Penicillin	1709	559	32.71	15.7	25615.23
Cancer	Penicillin	1631	529	32.43	15.7	24784.95
Obesity	Ibuprofen	1684	540	32.07	15.2	26238.01
Diabetes	Paracetamol	1649	523	31.72	15.6	26317.53
Arthritis	Aspirin	1748	553	31.64	15.7	25719.39
Arthritis	Ibuprofen	1676	530	31.62	15.4	25526.05
Hypertension	Lipitor	1675	527	31.46	15.4	25612.44

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
    total_admissions DESC
LIMIT 50;


patient_name	medical_condition	total_admissions	first_admission	last_discharge	total_duration_span	avg_stay_per_admission	avg_billing_per_stay
William Smith	Arthritis	10	2019-07-05	2023-05-20	1415	14.5	29283.54
Jennifer Smith	Obesity	10	2019-10-24	2023-02-05	1200	18.4	36585.42
Laura Davis	Obesity	10	2021-12-17	2024-02-26	801	8.5	30336.02
Michael Smith	Arthritis	10	2020-03-27	2024-04-13	1478	16.7	21720.03
Michael Williams	Hypertension	7	2019-12-05	2023-10-17	1412	12.3	20407.24
Michael Anderson	Arthritis	7	2019-08-16	2023-11-24	1561	21.1	19563.25
John Johnson	Arthritis	7	2019-07-24	2023-03-30	1345	9.3	17745.37
Christopher Smith	Arthritis	7	2020-08-06	2024-05-01	1364	10	15929.98
Michael Smith	Hypertension	7	2019-07-28	2023-06-15	1418	13.4	22358.18
Michael Johnson	Hypertension	7	2019-05-25	2023-04-22	1428	10.9	15434.4
James Crawford	Cancer	6	2020-07-10	2024-01-10	1279	14.8	23579.05
Amanda Wilson	Diabetes	6	2020-05-16	2023-12-26	1319	11.7	17071.61
Bryan Smith	Asthma	6	2019-05-29	2024-03-24	1761	11.7	27974.1
Lauren Johnson	Hypertension	6	2019-05-08	2024-05-23	1842	15.8	21529.38
Michael Martin	Asthma	6	2019-07-28	2022-09-27	1157	17.5	26715.82
Matthew Smith	Asthma	6	2019-07-18	2024-02-27	1685	16.8	13518.38
Adam Williams	Cancer	6	2019-07-01	2022-08-28	1154	20.8	31458.95
Michael Williams	Cancer	6	2020-05-11	2022-12-26	959	14.5	18290.99
Elizabeth Torres	Cancer	6	2020-09-01	2022-12-27	847	13.3	23998.78
John Williams	Hypertension	6	2020-09-19	2022-10-30	771	15.7	25116.27
John Miller	Cancer	6	2020-09-04	2023-11-16	1168	21	22712.75
Matthew Jones	Obesity	6	2021-03-14	2024-02-23	1076	24.5	14945.61
Jeffrey Johnson	Asthma	6	2019-07-28	2020-01-18	174	11.3	25242.68
David Lopez	Arthritis	6	2019-08-27	2024-05-01	1709	24.2	25613.76
David Smith	Hypertension	6	2021-11-20	2024-03-06	837	21.3	22880.64
Michael Smith	Asthma	6	2019-07-21	2024-03-28	1712	16.2	37656.24
John Smith	Hypertension	6	2020-08-22	2024-01-09	1235	14.2	24404.23
Christopher Jones	Hypertension	6	2020-06-16	2023-12-29	1291	14.2	30430.86
Robert Smith	Obesity	6	2021-04-24	2023-05-26	762	13.3	30791.4
Lisa Smith	Diabetes	6	2019-12-03	2024-01-29	1518	18.3	19194.16
Jennifer Jones	Obesity	6	2021-03-07	2023-12-10	1008	10.7	36754.98
James Garcia	Diabetes	6	2019-06-23	2024-01-11	1663	16.2	23083.4
Victoria Johnson	Cancer	6	2020-05-21	2022-12-30	953	13.7	20360.27
Robert Long	Arthritis	6	2019-12-24	2022-06-23	912	14	28714.69
Jason Smith	Arthritis	5	2021-09-07	2024-01-23	868	7.2	39814.61
John Anderson	Asthma	5	2020-01-23	2023-09-18	1334	25.2	34731.25
Ryan Harris	Diabetes	5	2020-12-01	2023-08-14	986	12.2	14587.84
James Williams	Hypertension	5	2023-01-10	2024-04-01	447	15.4	34108.17
Amanda Smith	Asthma	5	2021-04-08	2024-03-27	1084	5.8	10761.57
James King	Diabetes	5	2019-07-30	2023-09-30	1523	22.6	34052.5
Christopher Jackson	Cancer	5	2020-12-17	2023-07-19	944	12.4	28064.76
Kenneth Smith	Hypertension	5	2021-04-22	2023-06-13	782	6.8	8623.34
Amy Smith	Diabetes	5	2021-05-08	2023-07-10	793	11	18104.05
Robert Smith	Cancer	5	2020-05-09	2023-10-14	1253	12.2	19021.34
Anne Smith	Arthritis	5	2020-09-06	2023-11-02	1152	17.2	26152.28
Brian Smith	Hypertension	5	2019-08-07	2024-04-07	1705	15	30016.03
Mary Jones	Arthritis	5	2019-07-09	2023-10-27	1571	12.4	20509.92
Michael Miller	Diabetes	5	2019-06-03	2024-02-10	1713	15.2	38229.96
Anthony Davis	Asthma	5	2020-05-26	2024-02-25	1370	6.4	18289.31
Justin Smith	Obesity	5	2020-03-31	2023-10-21	1299	16.4	33212.52