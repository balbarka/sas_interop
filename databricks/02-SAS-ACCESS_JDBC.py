# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # 
# MAGIC
# MAGIC When using [SAS/ACCESS Interface for JDBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_037/acreldb/n0zm6fjwtgsnrzn1fegvyhl3yrwd.htm) you can create a **libname** that will allow both read and write interface with a Databricks runtime. There is an example, <a href="$../sas/jdbc_libname.sas" target="_blank">jdbc_libname.sas</a>, as a SAS Script. It uses SAS Variables in the following form:
# MAGIC
# MAGIC ```
# MAGIC libname jdbc JDBC 
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="&CLASS_PATH"
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
# MAGIC         ssl=1;AuthMech=3;httpPath=&HTTP_PATH;
# MAGIC         ConnCatalog=&CATALOG;
# MAGIC         ConnSchema=&SCHEMA;"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;
# MAGIC ```
# MAGIC
# MAGIC | Variable         | Definition |
# MAGIC | ---------------- | ----------------- |
# MAGIC | `CLASS_PATH`     | The folder path where the [Datbaricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) is saved. This can be ommitted if saved to `data-drivers/jdbc`. |
# MAGIC | `DBR_HOST`       | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> JDBC URL. </br>It is also found in the URL of the workspace.                           |
# MAGIC | `HTTP_PATH`      | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> HTTP path. |
# MAGIC | `DBR_TOKEN`      | This is your [Databricks Personal Access Token](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication) which is found in [User Settings](#setting/account/token) -> Access Tokens |
# MAGIC | `CATALOG`        | If using Unity Catalog set to catalog to be used. You can ommit `ConnCatalog` if not using Unity Catalog. |
# MAGIC | `SCHEMA`         | This is the database for the libname. |
# MAGIC
# MAGIC **NOTE**: While SAS/ACCESS for JDBC works well for most non-distributed reads from Delta, you will want to consider using caslib for large data reads and distributed transforms in SAS.
# MAGIC
# MAGIC **NOTE**: SAS/ACCESS for JDBC will not perform well for large data writes (ie. > 10GB). Consider using <a href="$./04-SAS-ACCESS_Spark" target="_blank">04-SAS-ACCESS_Spark</a> with `BULKLOAD=YES` if you need to write large datasets from SAS to DeltaLake.
# MAGIC
# MAGIC **NOTE**: None of the Variables above are set in these example since they are all set in SAS Studio Job Execution autoexec statement.
# MAGIC
# MAGIC **NOTE**: Databricks provides a [JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) available for download.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.jdbc_cars;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC %let HTTP_PATH=&CLUSTER_PATH;
# MAGIC
# MAGIC libname jdbc JDBC 
# MAGIC     driverclass="com.databricks.client.jdbc.Driver"
# MAGIC     classpath="/export/sas-viya/data/drivers/"
# MAGIC     URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=&HTTP_PATH;ConnCatalog=sas_interop;ConnSchema=demo;"
# MAGIC     user="token" 
# MAGIC     password=&DBR_TOKEN;
# MAGIC
# MAGIC data jdbc.jdbc_cars;
# MAGIC set sashelp.cars;
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sas_interop.demo.jdbc_cars;
