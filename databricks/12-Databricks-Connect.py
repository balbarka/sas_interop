# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This will walk through running a spark session from Jupyter or SAS PROC.
# MAGIC
# MAGIC This will show how to run spark code where the driver is local. The option to run in SAS is to be able to execute Spark code explicitly, but doesn't offer a goos interactive user experience.
# MAGIC
# MAGIC The interactive use experience should use Jupyter:
# MAGIC
# MAGIC [Databricks-Connect](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect)
# MAGIC
# MAGIC from databricks.connect import DatabricksSession
# MAGIC
# MAGIC ``` python
# MAGIC spark = DatabricksSession.builder.remote(
# MAGIC   host       = "https://<workspace-instance-name>",
# MAGIC   token      = "<access-token-value>",
# MAGIC   cluster_id = "<cluster-id>"
# MAGIC ).getOrCreate()
# MAGIC
# MAGIC #or
# MAGIC
# MAGIC from databricks.connect import DatabricksSession
# MAGIC
# MAGIC spark = DatabricksSession.builder.remote(
# MAGIC   "sc://<workspace-instance-name>:443/;token=<access-token-value>;x-databricks-cluster-id=<cluster-id>"
# MAGIC ).getOrCreate()
# MAGIC ```
