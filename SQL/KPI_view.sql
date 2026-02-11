DROP VIEW IF EXISTS mart.kpi_monthly_v2;

CREATE VIEW mart.kpi_monthly_v2 AS
WITH orders_u AS (
  SELECT
    c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp AS ts
  FROM stg.orders o
  JOIN stg.customers c
    ON c.customer_id = o.customer_id
  WHERE o.order_purchase_timestamp IS NOT NULL
),

-- 1) Заказы по месяцам 
orders_monthly AS (
  SELECT
    DATE_TRUNC('month', ts) AS month,
    COUNT(DISTINCT order_id) AS orders
  FROM orders_u
  GROUP BY 1
),

-- 2) Первый заказ клиента 
first_order AS (
  SELECT
    customer_unique_id,
    MIN(ts) AS first_ts,
    DATE_TRUNC('month', MIN(ts)) AS first_month
  FROM orders_u
  GROUP BY 1
),

-- 3) Клиент-месяц 
customer_months AS (
  SELECT DISTINCT
    customer_unique_id,
    DATE_TRUNC('month', ts) AS month
  FROM orders_u
),

customer_months_flagged AS (
  SELECT
    cm.month,
    cm.customer_unique_id,
    CASE WHEN cm.month = fo.first_month THEN 1 ELSE 0 END AS is_new_customer
  FROM customer_months cm
  JOIN first_order fo
    ON fo.customer_unique_id = cm.customer_unique_id
),

customers_monthly AS (
  SELECT
    month,
    COUNT(*) AS customers,
    SUM(is_new_customer) AS new_customers,
    COUNT(*) - SUM(is_new_customer) AS repeat_customers,
    ROUND(
      100.0 * (COUNT(*) - SUM(is_new_customer)) / NULLIF(COUNT(*), 0)
    , 2) AS repeat_rate_pct
  FROM customer_months_flagged
  GROUP BY 1
)

SELECT
  c.month,
  o.orders,
  c.customers,
  c.new_customers,
  c.repeat_customers,
  c.repeat_rate_pct
FROM customers_monthly c
JOIN orders_monthly o USING (month)
ORDER BY c.month;


COPY (
  SELECT month, orders, customers, new_customers, repeat_customers, repeat_rate_pct
  FROM mart.kpi_monthly_v2
  ORDER BY month
) TO 'C:\Brazil_dataset\kpi_monthly_v2.csv'
WITH (FORMAT csv, HEADER true);


SELECT month, customers, new_customers, repeat_customers, repeat_rate_pct
FROM mart.kpi_monthly_v2
ORDER BY month DESC
LIMIT 6;


SELECT * FROM mart.kpi_monthly_v2 LIMIT 12;
SELECT MIN(month), MAX(month) FROM mart.kpi_monthly;
