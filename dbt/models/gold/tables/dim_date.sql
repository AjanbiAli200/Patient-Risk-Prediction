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
