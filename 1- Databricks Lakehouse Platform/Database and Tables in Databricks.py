# Databricks notebook source
# MAGIC %md
# MAGIC ## Managed Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE TABLE managed_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_defaulthive_metastore.default.external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_default;

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/demo/external_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/managed_default

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table external_default;

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/demo/external_default

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from external_default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED new_default
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE managed_new_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC   
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_new_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC   
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_new_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_new_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_new_default;
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/new_default.db/managed_new_default

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/demo/external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_new_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/user/hive/warehouse/new_default.db/external_new_default';
# MAGIC   
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/new_default.db/external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/new_default.db/external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from external_new_default;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Schemas in Custom Location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA custom
# MAGIC LOCATION 'dbfs:/Pratik/shared/custom.db'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE database extended custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE managed_custom
# MAGIC   (width INT, length INT, height INT);
# MAGIC   
# MAGIC INSERT INTO managed_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_custom
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_custom';
# MAGIC   
# MAGIC INSERT INTO external_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_custom;
# MAGIC DROP TABLE external_custom;

# COMMAND ----------

# MAGIC %fs ls dbfs:/Pratik/shared/custom.db/managed_custom

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/demo/external_custom

# COMMAND ----------


