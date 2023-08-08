# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Parquet Table using ADLS `FILENAME <table> parquet`
# MAGIC
# MAGIC **NOTE**: ADLS is not yet supported in the Parquet plug-in, see [documentation](https://documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/n1p9l5zmpodbmyn14omvxe4dt4wx.htm). 
# MAGIC
# MAGIC ```
# MAGIC ERROR: Exception occurred while writing to Parquet table: ADLS is not currently supported in the Parquet plug-in..
# MAGIC ERROR: Failed to open ADLS_PQ.cars_pq.
# MAGIC ```
# MAGIC
# MAGIC **TODO**: Write demo that generates this exception from ADLS.
# MAGIC
# MAGIC **NOTE**: Confirmation from SAS that ADLS parquet is supported after 2023.01 in SAS Viya.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are some limitations to the folder structure conventions used by SAS.
# MAGIC
# MAGIC To load CAS from S3-parquet data file, files and sub-folder names must have .parquet extension.
# MAGIC
# MAGIC This conventions does introduce a complexity where the partitions of the table are no longer automatically read by databricks and must be manually altered. Thus, a partitioned parquet table does have some complexity to manage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## TODO: create aws S3 bucket and write AWS S3 Parquet demo
