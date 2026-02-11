CREATE SCHEMA IF NOT EXISTS stg;
DROP TABLE IF EXISTS stg.orders;

CREATE TABLE stg.orders (
  order_id TEXT,
  customer_id TEXT,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

COPY stg.orders
FROM 'C:\Brazil_dataset\olist_orders_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

SELECT COUNT(*) FROM stg.orders;
SELECT order_status, COUNT(*) FROM stg.orders GROUP BY 1 ORDER BY 2 DESC;
SELECT MIN(order_purchase_timestamp), MAX(order_purchase_timestamp) FROM stg.orders;

CREATE TABLE IF NOT EXISTS stg.customers (
  customer_id TEXT,
  customer_unique_id TEXT,
  customer_zip_code_prefix TEXT,
  customer_city TEXT,
  customer_state TEXT
);

TRUNCATE TABLE stg.customers;

COPY stg.customers
FROM 'C:\Brazil_dataset\olist_customers_dataset.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

SELECT COUNT(*) FROM stg.customers;
SELECT COUNT(DISTINCT customer_id) FROM stg.customers;
SELECT COUNT(DISTINCT customer_unique_id) FROM stg.customers;

CREATE INDEX IF NOT EXISTS ix_orders_customer_id ON stg.orders(customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_purchase_ts ON stg.orders(order_purchase_timestamp);

CREATE INDEX IF NOT EXISTS ix_customers_customer_id ON stg.customers(customer_id);
CREATE INDEX IF NOT EXISTS ix_customers_unique_id ON stg.customers(customer_unique_id);


CREATE SCHEMA IF NOT EXISTS mart;

DROP VIEW IF EXISTS mart.customer_first_order;

CREATE VIEW mart.customer_first_order AS
SELECT
  c.customer_unique_id,
  MIN(o.order_purchase_timestamp) AS first_order_ts,
  DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS cohort_month
FROM stg.orders o
JOIN stg.customers c
  ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1;


sELECT * FROM mart.customer_first_order
LIMIT 5



DROP VIEW IF EXISTS mart.customer_order_months;

CREATE VIEW mart.customer_order_months AS
SELECT DISTINCT
  c.customer_unique_id,
  DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month
FROM stg.orders o
JOIN stg.customers c
  ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL;

SELECT * FROM mart.customer_order_months LIMIT 10;



WITH cs AS (
  SELECT cohort_month, COUNT(*) AS cohort_customers
  FROM mart.customer_first_order
  GROUP BY 1
),
m1 AS (
  SELECT cohort_month, customers
  FROM mart.cohort_retention
  WHERE month_number = 1
)
SELECT
  cs.cohort_month,
  cs.cohort_customers,
  m1.customers AS customers_m1,
  ROUND(100.0 * m1.customers / cs.cohort_customers, 2) AS calc_retention_m1
FROM cs
JOIN m1 USING (cohort_month)
WHERE cs.cohort_month = '2017-08-01';



