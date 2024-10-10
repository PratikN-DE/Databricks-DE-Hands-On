# Databricks notebook source
# MAGIC %md
# MAGIC #creating delta lake tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table employees
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Explorer

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES 
# MAGIC   (1, "Adam", 3500.0),
# MAGIC   (2, "Sarah", 4020.5);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (3, "John", 2999.3),
# MAGIC   (4, "Thomas", 4000.3);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (5, "Anna", 2500.0);
# MAGIC
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC   (6, "Kim", 6200.3);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring Table Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring Table Directory

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC ## updating table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 100
# MAGIC WHERE name LIKE "A%"

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## exploring table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees@v4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees@v5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql describe detail employees;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees to version as of 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE Command

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC zorder by id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM Command

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v1
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE history employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees@v10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/employees

# COMMAND ----------


