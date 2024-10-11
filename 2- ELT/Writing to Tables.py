# Databricks notebook source
# MAGIC %run /Users/pratik.nandankar1@gmail.com/Include/copy-datasets.py

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overwriting Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE orders
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customers c
# MAGIC USING customers_updates u
# MAGIC ON c.customer_id = u.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
# MAGIC   UPDATE SET email = u.email, updated = u.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_updates
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv-new",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM books_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO books b
# MAGIC USING books_updates u
# MAGIC ON b.book_id = u.book_id AND b.title = u.title
# MAGIC WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books where category = 'Computer Science'

# COMMAND ----------


