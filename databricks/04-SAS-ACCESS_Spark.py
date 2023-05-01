# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS/ACCESS Interface for Spark (BULKLOAD=NO)
# MAGIC
# MAGIC When using [SAS/ACCESS Interface for Spark](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1qzm30adels9wn1oze9mgyc5pwc.htm) you can create a **libname** that will allow both read and write interface with a Databricks runtime. 
# MAGIC
# MAGIC
# MAGIC There is an example, <a href="$../sas/jdbc_libname.sas" target="_blank">jdbc_libname.sas</a>, as a SAS Script. It uses SAS Variables in the following form:
# MAGIC
# MAGIC ```
# MAGIC libname CdtSpark spark
# MAGIC     driverClass="cdata.jdbc.databricks.DatabricksDriver"
# MAGIC     bulkload=NO
# MAGIC     url="jdbc:databricks:Server=&DBR_HOST;
# MAGIC          HTTPPath=&HTTP_PATH;Database=default;
# MAGIC          QueryPassthrough=true;Token=&DBR_TOKEN" ;
# MAGIC ```
# MAGIC
# MAGIC | Variable         | Definition |
# MAGIC | ---------------- | ----------------- |
# MAGIC | `DBR_HOST`       | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> JDBC URL. </br>It is also found in the URL of the workspace.                           |
# MAGIC | `HTTP_PATH`      | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> HTTP path. |
# MAGIC | `DBR_TOKEN`      | This is your [Databricks Personal Access Token](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication) which is found in [User Settings](#setting/account/token) -> Access Tokens |
# MAGIC
# MAGIC **NOTE**: While SAS/ACCESS for Spark works well for most non-distributed reads from Delta, you will want to consider using caslib for large data reads and distributed transforms in SAS.
# MAGIC
# MAGIC **NOTE**: SAS/ACCESS for Spark (BULKLOAD=NO) will not perform well for large data writes (ie. > 10GB). Consider using <a href="$./05-SAS-ACCESS_Spark_bl" target="_blank">05-SAS-ACCESS_Spark_bl</a> with `BULKLOAD=YES` if you need to write large datasets from SAS to DeltaLake.
# MAGIC
# MAGIC **NOTE**: None of the Variables above are set in these example since they are all set in SAS Studio Job Execution autoexec statement.
# MAGIC
# MAGIC **NOTE**: Databricks provides a [JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) available for download.
# MAGIC
# MAGIC **NOTE**: The cdat driver doesn't have a way to set catalog, therefore it is not possible to use this driver for unity catalog. You must instead use `hive_metastore`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.demo;
# MAGIC DROP TABLE IF EXISTS hive_metastore.demo.spark_cars;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC libname spark spark
# MAGIC     driverClass=&MYDRIVERCLASS
# MAGIC     bulkload=NO
# MAGIC     url="jdbc:databricks:Server=&DBR_HOST;
# MAGIC      HTTPPath=&CLUSTER_PATH;Database=default;
# MAGIC          QueryPassthrough=true;Token=&DBR_TOKEN" ;

# COMMAND ----------


