# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Parquet Table using ADLS `FILENAME <table> parquet`
# MAGIC
# MAGIC **NOTE**: ADLS is not yet supported in the Parquet plug-in. 
# MAGIC
# MAGIC ```
# MAGIC ERROR: Exception occurred while writing to Parquet table: ADLS is not currently supported in the Parquet plug-in..
# MAGIC ERROR: Failed to open ADLS_PQ.cars_pq.
# MAGIC ```
# MAGIC
# MAGIC **TODO**: Write demo that generates this exception.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are some limitations to the folder structure conventions used by SAS.
# MAGIC
# MAGIC To load CAS from S3-parquet data file, files and sub-folder names must have .parquet extension.
# MAGIC
# MAGIC This conventions does introduce a complexity where the partitions of the table are no longer automatically read by databricks and must be manually altered. Thus, a partitioned parquet table does have some complexity to manage.

# COMMAND ----------


