# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC TODO: Write considerations when using shared external location with SAS.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS sas_interop
# MAGIC MANAGED LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/managed';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sas_interop.demo
# MAGIC MANAGED LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/managed/demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sas_interop.demo.test (a INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED sas_interop.demo.test;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN sas_interop.demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sas_interop.demo.cars;
