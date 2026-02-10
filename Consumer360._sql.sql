CREATE TABLE raw_sales (

    invoice_no    VARCHAR(20),
    product_id    VARCHAR(20),
    description   VARCHAR(255),
    quantity      INT,
    invoice_date  TIMESTAMP,
    unit_price      DECIMAL(10,2),
    customer_id   INT,
    country       VARCHAR(50)
);

--Create Clean Table
CREATE TABLE cleaned_sales AS
SELECT
    invoice_no,
    product_id,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country  
FROM raw_sales
WHERE customer_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0;
  SELECT *FROM cleaned_sales;

  --Add Revenue Column
ALTER TABLE cleaned_sales
ADD COLUMN revenue DECIMAL(10,2);

--Populate Revenue Column
UPDATE cleaned_sales
SET revenue = quantity * unit_price;

--Create fact table
CREATE TABLE fact_sales AS
SELECT DISTINCT
  invoice_no ,
  customer_id,
  product_id ,
  quantity ,
  total_amount,
  invoice_date 
  FROM cleaned_sales;

SELECT * FROM fact_sales

--Customer Dimension table
CREATE TABLE dim_customer AS
SELECT DISTINCT
  customer_id ,
  country
  FROM cleaned_sales; 
SELECT* FROM dim_customer

--Product Dimension
INSERT INTO dim_product AS
SELECT DISTINCT
    product_id,
    description
FROM cleaned_sales;
SELECT* FROM dim_product;

--create view
CREATE VIEW single_customer_view AS
SELECT
    invoice_no,
    product_id,
    description,
    quantity,
    invoice_date,
    unit_price,
    customer_id,
    country,
    (quantity * unit_price) AS revenue
FROM raw_sales
WHERE customer_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0;
select * from single_customer_view;

--monthly sales trend
SELECT 
    EXTRACT(YEAR FROM invoice_date) AS year,
    EXTRACT(MONTH FROM invoice_date) AS month,
    SUM(revenue) AS total_sales
FROM fact_sales
GROUP BY 
    EXTRACT(YEAR FROM invoice_date),
    EXTRACT(MONTH FROM invoice_date)
ORDER BY year, month;

--RFM analysis
WITH RFM AS (
    SELECT 
        customer_id,
        -- Recency: days since last purchase
        (CURRENT_DATE - MAX(invoice_date)) AS recency,
        -- Frequency: number of orders
        COUNT(DISTINCT product_id) AS frequency,
        -- Monetary: total spent
        SUM(revenue) AS monetary
    FROM fact_sales
    GROUP BY customer_id
)
SELECT * FROM RFM ORDER BY recency;

--Customer churn analysis
WITH RFM AS (
    SELECT 
        customer_id,
        (CURRENT_DATE - MAX(invoice_date)) AS recency,
        COUNT(DISTINCT product_id) AS frequency,
        SUM(revenue) AS monetary
    FROM fact_sales
    GROUP BY customer_id
)
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    CASE 
        WHEN recency > 180 THEN 'Churned'
        ELSE 'Active'
    END AS status
FROM RFM
ORDER BY recency DESC;