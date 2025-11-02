{{ config(materialized='view') }}

SELECT
    row_number() OVER (ORDER BY md5(insurance_provider)) AS insurance_sk,
    insurance_provider,
    COUNT(*) AS total_claims,
    ROUND(SUM(billing_amount), 2) AS total_billing,
    ROUND(AVG(billing_amount), 2) AS avg_billing,
    MAX(billing_amount) AS max_billing
FROM {{ source("source_silver", "healthcare") }}
GROUP BY insurance_provider
ORDER BY total_billing DESC;
