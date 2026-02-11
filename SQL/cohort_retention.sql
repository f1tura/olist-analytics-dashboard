DROP VIEW IF EXISTS mart.cohort_retention;

CREATE VIEW mart.cohort_retention AS
WITH base AS (
  SELECT
    f.customer_unique_id,
    f.cohort_month,
    m.order_month,
    (
      EXTRACT(YEAR FROM m.order_month) * 12 + EXTRACT(MONTH FROM m.order_month)
      - (EXTRACT(YEAR FROM f.cohort_month) * 12 + EXTRACT(MONTH FROM f.cohort_month))
    )::int AS month_number
  FROM mart.customer_first_order f
  JOIN mart.customer_order_months m
    ON m.customer_unique_id = f.customer_unique_id
),
cohort_size AS (
  SELECT cohort_month, COUNT(*) AS cohort_customers
  FROM mart.customer_first_order
  GROUP BY 1
)
SELECT
  b.cohort_month,
  b.month_number,
  COUNT(DISTINCT b.customer_unique_id) AS customers,
  ROUND(
    100.0 * COUNT(DISTINCT b.customer_unique_id) / cs.cohort_customers
  , 2) AS retention_pct
FROM base b
JOIN cohort_size cs
  ON cs.cohort_month = b.cohort_month
WHERE b.month_number >= 0
GROUP BY 1,2, cs.cohort_customers
ORDER BY 1,2;

SELECT cohort_month, retention_pct
FROM mart.cohort_retention
WHERE month_number = 0
ORDER BY cohort_month
LIMIT 20;

SELECT *
FROM mart.cohort_retention
WHERE cohort_month = '2017-10-01'
ORDER BY month_number;


COPY (
  SELECT cohort_month, month_number, customers, retention_pct
  FROM mart.cohort_retention
  ORDER BY cohort_month, month_number
) TO 'C:\Brazil_dataset\cohort_retention.csv'
WITH (FORMAT csv, HEADER true);
