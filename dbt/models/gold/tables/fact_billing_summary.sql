{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hospital,
        insurance_provider,
        ROUND(SUM(billing_amount), 2) AS total_billed,
        ROUND(AVG(billing_amount), 2) AS avg_bill,
        COUNT(*) AS total_patients
    FROM {{ source("source_silver","healthcare") }}
    GROUP BY 1,2
)
SELECT
    row_number() OVER (ORDER BY md5(concat(hospital, insurance_provider))) AS billing_sk,
    hospital,
    insurance_provider,
    total_billed,
    avg_bill,
    total_patients
FROM base;
