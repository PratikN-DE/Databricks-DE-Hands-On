# Databricks notebook source
# MAGIC %md
# MAGIC ## Querying JSON

# COMMAND ----------

# MAGIC %run /Users/pratik.nandankar1@gmail.com/Include/copy-datasets.py

# COMMAND ----------

print(dataset_bookstore)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")

# COMMAND ----------

display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC     input_file_name() source_file
# MAGIC   FROM json.`${dataset.bookstore}/customers-json`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying text Format

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying binaryFile Format

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_csv
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   sep = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations of Non-Delta Tables

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

(spark.read
.table("books_csv")
.write
.mode("append")
.format("csv")
.option('header', 'true')
.option('delimiter', ';')
.save(f"{dataset_bookstore}/books-csv"))

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS Statements

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE books_unparsed AS
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_unparsed;

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED books_unparsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_tmp_vw
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE books AS
# MAGIC   SELECT * FROM books_tmp_vw;
# MAGIC
# MAGIC select * from books;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books_delta as 
# MAGIC     select * from books_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_delta;

# COMMAND ----------

(spark.read
.table("books_delta")
.write
.mode("append")
.save("dbfs:/user/hive/warehouse/books_delta/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from books_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_delta;

# COMMAND ----------

# MAGIC %sql REFRESH TABLE books_delta;

# COMMAND ----------


