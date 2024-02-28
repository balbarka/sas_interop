# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS/ACCESS Interface for Spark (BULKLOAD=NO)
# MAGIC
# MAGIC When using [SAS/ACCESS Interface for Spark](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1qzm30adels9wn1oze9mgyc5pwc.htm) you can create a **libname** that will allow both read and write interface with a Databricks runtime. 
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/spark_connection.png" alt="spark_connection" width="600px">
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
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Below is an example of creating a table in delta without using bulk load:

# COMMAND ----------

# MAGIC %%SAS
# MAGIC options azuretenantid="&AZ_TENANT_ID";
# MAGIC
# MAGIC libname spark_db clear;
# MAGIC libname spark_db spark
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="/access-clients/jdbc"
# MAGIC    bulkload=NO 
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443/default;
# MAGIC         transportMode=http;
# MAGIC         ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;
# MAGIC         ConnCatalog=sas_interop;
# MAGIC         ConnSchema=demo;EnableArrow=1;
# MAGIC         UseNativeQuery=1"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;
# MAGIC
# MAGIC proc sql;
# MAGIC DROP TABLE spark_db.spark_cars;
# MAGIC quit;
# MAGIC
# MAGIC data spark_db.spark_cars;
# MAGIC set sashelp.cars;
# MAGIC run;
# MAGIC
# MAGIC proc sql;
# MAGIC select * from spark_db.spark_cars(obs=3);
# MAGIC quit;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sas_interop.demo.spark_cars LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SAS/ACCESS Interface for Spark (BULKLOAD=YES)
# MAGIC
# MAGIC When using [SAS/ACCESS Interface for Spark](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1qzm30adels9wn1oze9mgyc5pwc.htm) you also have the ability to define the conection with Bulk Load. This improves the perfprmance of writing larger tables by goin though the following steps:
# MAGIC
# MAGIC  * Create a target table (if necessary)
# MAGIC  * Write SAS output to a cloud blob storage as text files
# MAGIC  * Run [Load Data](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-dml-load) to insert files in to Delta table. 
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/spark_bl.png" alt="spark_bl" width="600px">
# MAGIC
# MAGIC When bulkloading to Azure Databricks, you must provide additinoal configurations [Azure Databricks Spark BULKLOADING](https://go.documentation.sas.com/doc/en/pgmsascdc/v_038/acreldb/n1udyznblny75qn1x7ng2y4wahxf.htm#n0wn1jzs1z6ekun17vwi342jxcgz)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC libname sp_db_bl clear;
# MAGIC libname sp_db_bl spark
# MAGIC    classpath="/access-clients/jdbc"
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    bulkload=YES
# MAGIC    bl_applicationid="&ADLS_APPLICATION_ID"
# MAGIC    bl_accountname="&ADLS_ACCOUNT_NAME"
# MAGIC    bl_filesystem="sas-interop-tmp"
# MAGIC    bl_folder="external/demo/_tmp"
# MAGIC    BL_DELETE_DATAFILE=NO
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443/default;
# MAGIC         transportMode=http;
# MAGIC         ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;
# MAGIC         ConnCatalog=sas_interop;
# MAGIC         ConnSchema=demo;EnableArrow=1;
# MAGIC         UseNativeQuery=1;
# MAGIC         UID=token;
# MAGIC         PWD=&DBR_TOKEN";
# MAGIC
# MAGIC
# MAGIC proc sql;
# MAGIC DROP TABLE sp_db_bl.spark_cars_bl;
# MAGIC quit;
# MAGIC
# MAGIC data sp_db_bl.spark_cars_bl;
# MAGIC set sashelp.cars;
# MAGIC run;
# MAGIC
# MAGIC proc sql;
# MAGIC select * from sp_db_bl.spark_cars_bl(obs=3);
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Spark Data Connector (TODO)
# MAGIC
# MAGIC Just like JDBC and ODBC, there is a Data Connector for spark that can be used to read data into CAS. However, when reading the [documentation](https://documentation.sas.com/doc/en/pgmsascdc/v_035/casref/p02gmw66o8gtonn1astbqdzbjgq6.htm) you will see that it: **Enables you to transfer data between CAS and Spark that exists on a file system.**.
# MAGIC
# MAGIC **NOTE**: Parallelization on read / write isn't possible. **TODO**: See if newest SAS Versions have added this as a feature.
# MAGIC
