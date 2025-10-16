-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE IDENTIFIER(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Intro example: Area of rectangle

-- COMMAND ----------

CREATE OR REPLACE PROCEDURE area_of_rectangle
    (IN x INT, y INT, OUT area INT, INOUT acc INT)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    SET area = x * y;
    SET acc = acc + area;
END;

-- COMMAND ----------

DECLARE OR REPLACE VAR acc INT = 0;
DECLARE OR REPLACE VAR area_1 INT;
DECLARE OR REPLACE VAR area_2 INT;

CALL area_of_rectangle(10, 10, area_1, acc);
CALL area_of_rectangle(20, 20, area_2, acc);

SELECT area_1, area_2, acc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ETL: Preparing Silver or Gold layer tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data preparation

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_raw(sale_id STRING, sale_date_str STRING, customer_id STRING, amount DECIMAL(8, 2), origin STRING);
CREATE OR REPLACE TABLE sales_clean(sale_id STRING, sale_date DATE, customer_id STRING, amount DECIMAL(8, 2), origin STRING);
CREATE OR REPLACE TABLE etl_log(etl_timestamp TIMESTAMP, date_from DATE, date_to DATE, origin STRING);

-- COMMAND ----------

INSERT INTO sales_raw VALUES
  ("sale_0", "2025-07-29", "customer_1", 100.0, "Mobile App"),
  ("sale_1", " 2025-07-29 ", "customer_3", 200.0, " Store"),
  ("sale_2", "2025-07-30", "customer_2 ", 50.5, "Mobile App "),
  ("sale_3", "2025-07-31 ", "customer_0", 100.0, " Web App ");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stored procedure creation

-- COMMAND ----------

CREATE OR REPLACE PROCEDURE etl_load_sales_by_range_and_source (
    IN p_start_date DATE,
    IN p_end_date DATE,
    IN p_origin STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Clean and insert data into sales_clean
    INSERT INTO sales_clean (sale_id, sale_date, customer_id, amount, origin)
    SELECT
        sale_id,
        CAST(TRIM(sale_date_str) AS DATE),  -- Clean and convert date
        TRIM(customer_id),                  -- Remove leading/trailing whitespace
        amount,
        TRIM(origin)
    FROM sales_raw
    WHERE
        CAST(TRIM(sale_date_str) AS DATE) BETWEEN p_start_date AND p_end_date
        AND TRIM(origin) = p_origin;

    -- Log the ETL operation
    INSERT INTO etl_log (etl_timestamp, date_from, date_to, origin)
    VALUES (run_timestamp, p_start_date, p_end_date, p_origin);
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stored procedure execution

-- COMMAND ----------

CALL etl_load_sales_by_range_and_source(date('2025-07-28'), date('2025-08-01'), 'Mobile App');

-- COMMAND ----------

SELECT * FROM sales_clean;

-- COMMAND ----------

SELECT * FROM etl_log;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data management: Update customer loyalty tier

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data preparation

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_customers(customer_id STRING, first_name STRING, last_name STRING, country STRING, loyalty_tier STRING);
CREATE OR REPLACE TABLE sales_transactions(transaction_id STRING, customer_id STRING, total_price DECIMAL(10, 2), transaction_type STRING);

-- COMMAND ----------

INSERT INTO sales_customers VALUES
    ("1000127", "Kalamarko", "Kajmanović", "Serbia", "Silver"),
    ("1000220", "Friedrich", "Kaiser", "Germany", "Silver"),
    ("1000261", "Clark", "Martin", "Canada", "Silver"),
    ("1000370", "Vida", "Srnić", "Serbia", "Gold"),
    ("1000378", "Stanko", "Rukavica", "Serbia", "Gold"),
    ("1000380", "Duško", "Seničanin", "Serbia", "Gold"),
    ("1000501", "Maksim", "Giletić", "Serbia", "Platinum");
INSERT INTO sales_transactions VALUES
    ("0000001", "1000127", 250, "Online"),
    ("0000002", "1000220", 330, "Cash"),
    ("0000003", "1000220", 100, "Debit Card"),
    ("0000004", "1000261", 400, "Credit Card"),
    ("0000005", "1000370", 550, "Cash"),
    ("0000006", "1000378", 750, "Cash"),
    ("0000007", "1000380", 300, "Online"),
    ("0000007", "1000380", 500, "Cash"),
    ("0000008", "1000501", 10000, "Debit Card");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stored procedures creation

-- COMMAND ----------

CREATE OR REPLACE PROCEDURE update_customer_loyalty_tier
    (IN p_customer_id INT)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE total_spend BIGINT;
    DECLARE tier STRING;

    -- Calculate total spend
    SET total_spend = (
      SELECT SUM(total_price)
      FROM sales_transactions
      WHERE customer_id = p_customer_id
    );

    -- Determine tier
    IF total_spend >= 1000
    THEN
      SET tier = 'Platinum';
    ELSEIF total_spend >= 500
    THEN
      SET tier = 'Gold';
    ELSEIF total_spend >= 200
    THEN
      SET tier = 'Silver';
    ELSE
       SET tier = 'Bronze';
    END IF;

    -- Update tier for the customer
    UPDATE sales_customers
    SET loyalty_tier = tier
    WHERE customer_id = p_customer_id;
END;

-- COMMAND ----------

CREATE OR REPLACE PROCEDURE update_customer_loyalty_tiers_by_country
    (IN p_country STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    FOR customer AS (SELECT DISTINCT customer_id FROM sales_customers WHERE country = p_country) DO
        CALL update_customer_loyalty_tier(customer.customer_id);
    END FOR;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Stored procedures execution

-- COMMAND ----------

CALL update_customer_loyalty_tiers_by_country('Serbia');
CALL update_customer_loyalty_tiers_by_country('Germany');
CALL update_customer_loyalty_tiers_by_country('Canada');

SELECT customer_id, first_name, last_name, country, loyalty_tier
FROM sales_customers
WHERE country IN ('Serbia', 'Germany', 'Canada');
