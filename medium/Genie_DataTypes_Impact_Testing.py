# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Create CUSTOMERS Table - WRONG TYPES
# MAGIC %sql
# MAGIC -- WRONG TYPES: CUSTOMERS TABLE
# MAGIC -- ONLY customer_id is STRING (the join key) - everything else is correct!
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbt_practice_demo.dbt_rn.customers_wrong_types (
# MAGIC   customer_id STRING,              
# MAGIC   registration_date DATE,         
# MAGIC   first_name STRING,             
# MAGIC   last_name STRING,               
# MAGIC   email STRING,                    
# MAGIC   age INT,                         
# MAGIC   is_active BOOLEAN,              
# MAGIC   is_premium BOOLEAN,              
# MAGIC   lifetime_value DECIMAL(10,2),    
# MAGIC   total_orders INT,               
# MAGIC   last_purchase_date DATE,         
# MAGIC   credit_limit DECIMAL(10,2)      
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create ORDERS Table - WRONG TYPES
# MAGIC %sql
# MAGIC -- WRONG TYPES: ORDERS TABLE
# MAGIC -- ONLY customer_id (JOIN key) and total_amount (aggregation) are STRING!
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbt_practice_demo.dbt_rn.orders_wrong_types (
# MAGIC   order_id INT,                   
# MAGIC   customer_id INT,                
# MAGIC   order_date DATE,              
# MAGIC   order_timestamp TIMESTAMP,      
# MAGIC   product_id INT,                
# MAGIC   product_name STRING,            
# MAGIC   quantity INT,                    
# MAGIC   unit_price DECIMAL(10,2),        
# MAGIC   discount_percent DECIMAL(5,2),   
# MAGIC   tax_amount DECIMAL(10,2),        
# MAGIC   total_amount STRING,            
# MAGIC   status STRING,                  
# MAGIC   is_shipped BOOLEAN             
# MAGIC ); 

# COMMAND ----------

# DBTITLE 1,Create CUSTOMERS Table - CORRECT TYPES
# MAGIC %sql
# MAGIC -- CORRECT TYPES: CUSTOMERS TABLE
# MAGIC -- All columns with appropriate data types
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbt_practice_demo.dbt_rn.customers_correct_types (
# MAGIC   customer_id INT,                  
# MAGIC   registration_date DATE,        
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   age INT,                       
# MAGIC   is_active BOOLEAN,             
# MAGIC   is_premium BOOLEAN,            
# MAGIC   lifetime_value DECIMAL(10,2),    
# MAGIC   total_orders INT,               
# MAGIC   last_purchase_date DATE,       
# MAGIC   credit_limit DECIMAL(10,2)      
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create ORDERS Table - CORRECT TYPES
# MAGIC %sql
# MAGIC -- CORRECT TYPES: ORDERS TABLE
# MAGIC -- All columns with appropriate data types
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbt_practice_demo.dbt_rn.orders_correct_types (
# MAGIC   order_id INT,                     
# MAGIC   customer_id INT,                  
# MAGIC   order_date DATE,                 
# MAGIC   order_timestamp TIMESTAMP,       
# MAGIC   product_id INT,                  
# MAGIC   product_name STRING,
# MAGIC   quantity INT,                   
# MAGIC   unit_price DECIMAL(10,2),         
# MAGIC   discount_percent DECIMAL(5,2),    
# MAGIC   tax_amount DECIMAL(10,2),         
# MAGIC   total_amount DECIMAL(10,2),       
# MAGIC   status STRING,
# MAGIC   is_shipped BOOLEAN                
# MAGIC );
# MAGIC
# MAGIC SELECT 'Orders table (correct types) created - ready for data' AS status;

# COMMAND ----------

# DBTITLE 1,Generate 50K Customers - WRONG TYPES
# MAGIC %sql
# MAGIC -- Generate 50,000 customers (wrong types)
# MAGIC -- ONLY customer_id is STRING - everything else uses proper types
# MAGIC
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.customers_wrong_types
# MAGIC SELECT 
# MAGIC   CAST(1000 + row_num AS STRING) AS customer_id,   
# MAGIC   DATE_ADD('2023-01-01', row_num) AS registration_date,
# MAGIC   CONCAT('Customer', CAST(row_num AS STRING)) AS first_name,
# MAGIC   CONCAT('Last', CAST(row_num AS STRING)) AS last_name,
# MAGIC   CONCAT('customer', CAST(row_num AS STRING), '@email.com') AS email,
# MAGIC   20 + (row_num % 50) AS age,                       
# MAGIC   CASE WHEN row_num % 3 = 0 THEN false ELSE true END AS is_active,   
# MAGIC   CASE WHEN row_num % 4 = 0 THEN true ELSE false END AS is_premium, 
# MAGIC   ROUND(500 + (row_num * 87.5), 2) AS lifetime_value,   
# MAGIC   1 + (row_num % 80) AS total_orders,              
# MAGIC   DATE_ADD('2024-01-01', (row_num % 90)) AS last_purchase_date,  
# MAGIC   5000 + (row_num * 250) AS credit_limit  
# MAGIC FROM (
# MAGIC   SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS row_num
# MAGIC   FROM (SELECT EXPLODE(SEQUENCE(1, 50000)) AS id)
# MAGIC );
# MAGIC
# MAGIC SELECT COUNT(*) AS total_customers FROM dbt_practice_demo.dbt_rn.customers_wrong_types;

# COMMAND ----------

# DBTITLE 1,Generate 50K Customers - CORRECT TYPES
# MAGIC %sql
# MAGIC -- Generate 50,000 customers (correct types)
# MAGIC -- customer_id as INT - the key difference!
# MAGIC
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.customers_correct_types
# MAGIC SELECT 
# MAGIC   1000 + row_num AS customer_id,              
# MAGIC   DATE_ADD('2023-01-01', row_num) AS registration_date,
# MAGIC   CONCAT('Customer', CAST(row_num AS STRING)) AS first_name,
# MAGIC   CONCAT('Last', CAST(row_num AS STRING)) AS last_name,
# MAGIC   CONCAT('customer', CAST(row_num AS STRING), '@email.com') AS email,
# MAGIC   20 + (row_num % 50) AS age,
# MAGIC   CASE WHEN row_num % 3 = 0 THEN false ELSE true END AS is_active,
# MAGIC   CASE WHEN row_num % 4 = 0 THEN true ELSE false END AS is_premium,
# MAGIC   ROUND(500 + (row_num * 87.5), 2) AS lifetime_value,
# MAGIC   1 + (row_num % 80) AS total_orders,
# MAGIC   DATE_ADD('2024-01-01', (row_num % 90)) AS last_purchase_date,
# MAGIC   5000 + (row_num * 250) AS credit_limit
# MAGIC FROM (
# MAGIC   SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS row_num
# MAGIC   FROM (SELECT EXPLODE(SEQUENCE(1, 50000)) AS id)
# MAGIC );
# MAGIC
# MAGIC SELECT COUNT(*) AS total_customers FROM dbt_practice_demo.dbt_rn.customers_correct_types;

# COMMAND ----------

# DBTITLE 1,Generate 500K Orders - WRONG TYPES
# MAGIC %sql
# MAGIC -- Generate 500,000 orders (wrong types)
# MAGIC -- ONLY customer_id and total_amount are STRING!
# MAGIC
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.orders_wrong_types
# MAGIC SELECT 
# MAGIC   1000 + row_num AS order_id,                     
# MAGIC   1000 + (row_num % 50000) AS customer_id, 
# MAGIC   --CAST(1000 + (row_num % 50000) AS STRING) AS customer_id,   
# MAGIC   DATE_ADD('2023-06-01', (row_num % 365)) AS order_date,    
# MAGIC   TIMESTAMP(CONCAT(DATE_ADD('2023-06-01', (row_num % 365)), ' ', CAST(8 + (row_num % 12) AS STRING), ':', CAST((row_num % 60) AS STRING), ':00')) AS order_timestamp,   
# MAGIC   101 + (row_num % 10) AS product_id,  
# MAGIC   CASE (row_num % 10)
# MAGIC     WHEN 0 THEN 'Laptop Pro'
# MAGIC     WHEN 1 THEN 'Wireless Mouse'
# MAGIC     WHEN 2 THEN 'Monitor 27"'
# MAGIC     WHEN 3 THEN 'Keyboard Mech'
# MAGIC     WHEN 4 THEN 'Webcam HD'
# MAGIC     WHEN 5 THEN 'Desk Setup Pro'
# MAGIC     WHEN 6 THEN 'Office Chair'
# MAGIC     WHEN 7 THEN 'USB Hub'
# MAGIC     WHEN 8 THEN 'Headphones Pro'
# MAGIC     ELSE 'Cable Pack'
# MAGIC   END AS product_name,  
# MAGIC   1 + (row_num % 5) AS quantity,  
# MAGIC   ROUND(49.99 + ((row_num % 10) * 150), 2) AS unit_price,  
# MAGIC   ROUND((row_num % 20) * 0.5, 2) AS discount_percent,  
# MAGIC   ROUND((49.99 + ((row_num % 10) * 150)) * (1 + (row_num % 5)) * 0.08, 2) AS tax_amount,  
# MAGIC   CAST(ROUND((49.99 + ((row_num % 10) * 150)) * (1 + (row_num % 5)) * 1.08 * (1 - ((row_num % 20) * 0.005)), 2) AS STRING) AS total_amount,  
# MAGIC   CASE 
# MAGIC     WHEN row_num % 10 < 7 THEN 'completed'
# MAGIC     WHEN row_num % 10 < 9 THEN 'shipped'
# MAGIC     ELSE 'processing'
# MAGIC   END AS status,                                 
# MAGIC   CASE WHEN row_num % 10 < 8 THEN true ELSE false END AS is_shipped   
# MAGIC FROM (
# MAGIC   SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS row_num
# MAGIC   FROM (SELECT EXPLODE(SEQUENCE(1, 500000)) AS id)
# MAGIC );
# MAGIC
# MAGIC SELECT COUNT(*) AS total_orders FROM dbt_practice_demo.dbt_rn.orders_wrong_types;

# COMMAND ----------

# DBTITLE 1,Generate 500K Orders - CORRECT TYPES
# MAGIC %sql
# MAGIC -- Generate 500,000 orders (correct types)
# MAGIC -- customer_id as INT and total_amount as DECIMAL - the key fixes!
# MAGIC
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.orders_correct_types
# MAGIC SELECT 
# MAGIC   1000 + row_num AS order_id,
# MAGIC   1000 + (row_num % 50000) AS customer_id,        
# MAGIC   DATE_ADD('2023-06-01', (row_num % 365)) AS order_date,
# MAGIC  TIMESTAMP(CONCAT(DATE_ADD('2023-06-01', (row_num % 365)), ' ', CAST(8 + (row_num % 12) AS STRING), ':', CAST((row_num % 60) AS STRING), ':00')) AS order_timestamp,
# MAGIC   101 + (row_num % 10) AS product_id,
# MAGIC   CASE (row_num % 10)
# MAGIC     WHEN 0 THEN 'Laptop Pro'
# MAGIC     WHEN 1 THEN 'Wireless Mouse'
# MAGIC     WHEN 2 THEN 'Monitor 27"'
# MAGIC     WHEN 3 THEN 'Keyboard Mech'
# MAGIC     WHEN 4 THEN 'Webcam HD'
# MAGIC     WHEN 5 THEN 'Desk Setup Pro'
# MAGIC     WHEN 6 THEN 'Office Chair'
# MAGIC     WHEN 7 THEN 'USB Hub'
# MAGIC     WHEN 8 THEN 'Headphones Pro'
# MAGIC     ELSE 'Cable Pack'
# MAGIC   END AS product_name,
# MAGIC   1 + (row_num % 5) AS quantity,
# MAGIC   ROUND(49.99 + ((row_num % 10) * 150), 2) AS unit_price,
# MAGIC   ROUND((row_num % 20) * 0.5, 2) AS discount_percent,
# MAGIC   ROUND((49.99 + ((row_num % 10) * 150)) * (1 + (row_num % 5)) * 0.08, 2) AS tax_amount,
# MAGIC   ROUND((49.99 + ((row_num % 10) * 150)) * (1 + (row_num % 5)) * 1.08 * (1 - ((row_num % 20) * 0.005)), 2) AS total_amount,  -- ✅ DECIMAL (THE FIX!)
# MAGIC   CASE 
# MAGIC     WHEN row_num % 10 < 7 THEN 'completed'
# MAGIC     WHEN row_num % 10 < 9 THEN 'shipped'
# MAGIC     ELSE 'processing'
# MAGIC   END AS status,
# MAGIC   CASE WHEN row_num % 10 < 8 THEN true ELSE false END AS is_shipped
# MAGIC FROM (
# MAGIC   SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS row_num
# MAGIC   FROM (SELECT EXPLODE(SEQUENCE(1, 500000)) AS id)
# MAGIC );
# MAGIC
# MAGIC SELECT COUNT(*) AS total_orders FROM dbt_practice_demo.dbt_rn.orders_correct_types;

# COMMAND ----------

# DBTITLE 1,Invalidate Cache - Insert/Delete Method
# MAGIC %sql
# MAGIC -- Orders Wrong Types
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.orders_wrong_types 
# MAGIC VALUES (999998, '9999987', CURRENT_DATE(), CURRENT_TIMESTAMP(), 999999, 'dummy', 1, '0', '0', '0', '0', 'dummy', false);
# MAGIC DELETE FROM dbt_practice_demo.dbt_rn.orders_wrong_types WHERE order_id = 999999;
# MAGIC
# MAGIC -- Customers Wrong Types (customer_id, registration_date, first_name, last_name, email, age, is_active, is_premium, lifetime_value, total_orders, last_purchase_date, credit_limit)
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.customers_wrong_types 
# MAGIC VALUES ('999998', CURRENT_DATE(), 'Dummy', 'User', 'dummy@test.com', 99, false, false, 0.00, 0, CURRENT_DATE(), 0.00);
# MAGIC DELETE FROM dbt_practice_demo.dbt_rn.customers_wrong_types WHERE customer_id = '999999';

# COMMAND ----------

# MAGIC %sql
# MAGIC --Run Analyze after testing genie behaviour after invalidate cache
# MAGIC ANALYZE TABLE dbt_practice_demo.dbt_rn.customers_wrong_types 
# MAGIC COMPUTE STATISTICS FOR COLUMNS customer_id;
# MAGIC
# MAGIC ANALYZE TABLE dbt_practice_demo.dbt_rn.orders_wrong_types 
# MAGIC COMPUTE STATISTICS FOR COLUMNS customer_id;

# COMMAND ----------

# DBTITLE 1,Invalidate Cache - Insert/Delete Method
# MAGIC %sql
# MAGIC -- Orders Correct Types
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.orders_correct_types 
# MAGIC VALUES (999999, 999999, CURRENT_DATE(), CURRENT_TIMESTAMP(), 999999, 'dummy', 1, 0, 0, 0, 0, 'dummy', false);
# MAGIC DELETE FROM dbt_practice_demo.dbt_rn.orders_correct_types WHERE order_id = 999999;
# MAGIC
# MAGIC -- Customers Correct Types (customer_id, registration_date, first_name, last_name, email, age, is_active, is_premium, lifetime_value, total_orders, last_purchase_date, credit_limit)
# MAGIC INSERT INTO dbt_practice_demo.dbt_rn.customers_correct_types 
# MAGIC VALUES (999999, CURRENT_DATE(), 'Dummy', 'User', 'dummy@test.com', 99, false, false, 0.00, 0, CURRENT_DATE(), 0.00);
# MAGIC DELETE FROM dbt_practice_demo.dbt_rn.customers_correct_types WHERE customer_id = 999999;
# MAGIC
# MAGIC SELECT 'Cache invalidated for all 4 tables via insert/delete' AS status;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC ANALYZE TABLE dbt_practice_demo.dbt_rn.customers_correct_types 
# MAGIC COMPUTE STATISTICS FOR COLUMNS customer_id;
# MAGIC
# MAGIC
# MAGIC ANALYZE TABLE dbt_practice_demo.dbt_rn.orders_correct_types 
# MAGIC COMPUTE STATISTICS FOR COLUMNS customer_id;