# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS Access JDBC
# MAGIC
# MAGIC SAS Access for JDBC is the most straight forward interoperability approach for SAS users accustom to working with traditional RBDMS systems. 
# MAGIC
# MAGIC Within SAS Viya, you are able to use [SAS/ACCESS](https://support.sas.com/en/software/sas-access.html) which is component software within SAS specific to connecting to data sources. Each of the supported datasource have one or more a SAS/Access engines. These engines are a component of SAS software that when configured and instantiated enable SAS to access and manipulate data sources (both external databases or file systems). When using [SAS/ACCESS Interface to JDBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1usgr00wc9cvln1gnyp1807qu17.htm), we must also provide a JDBC driver (in our case, the [Databricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download)). The SAS JDBC Engine ensures that is SAS commands properly call the JDBC driver using the JDBC API to run valid Databricks API commands. The effect is that SAS can now connect to Databricks same as any other Data source which simplifies code complexity and improves user expereience. Here is the general JDBC connection diagram from [SAS Documentation](https://documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p12qtje0ckagd5n1eyudktfp4n5m.htm): </br>
# MAGIC ![SAS JDBC DIagram](https://go.documentation.sas.com/api/docsets/acreldb/v_003/content/images/jdbcupdated.png?locale=en)
# MAGIC
# MAGIC In our case, JDBC Driver and Data Source is specific to Databricks. The Databricks Source can actually be in one of two instance types;
# MAGIC  * [Databricks Cluster](https://learn.microsoft.com/en-us/azure/databricks/clusters/) - for the demo, we will use a Databricks Cluster.
# MAGIC  * [Databricks SQL Warehouse](https://learn.microsoft.com/en-us/azure/databricks/sql/admin/create-sql-warehouse#what-is-a-sql-warehouse) - no example provided, but behaves same as Databricks cluster.
# MAGIC
# MAGIC
# MAGIC When using [SAS/ACCESS Interface for JDBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n0zm6fjwtgsnrzn1fegvyhl3yrwd.htm) you can create a **libname** that will allow both read and write interface with a Databricks runtime. There is an example, <a href="$../sas/02-SAS-ACCESS_JDBC_databricks.sas" target="_blank">02-SAS-ACCESS_JDBC_databricks.sas</a>, as a SAS Script. It uses SAS Variables in the following form:
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
# MAGIC | `CLASS_PATH`     | The folder path where the [Datbaricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) is saved.  |
# MAGIC | `DBR_HOST`       | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> JDBC URL. </br>It is also found in the URL of the workspace.                           |
# MAGIC | `HTTP_PATH`      | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> HTTP path. |
# MAGIC | `DBR_TOKEN`      | This is your [Databricks Personal Access Token](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication) which is found in [User Settings](#setting/account/token) -> Access Tokens |
# MAGIC | `CATALOG`        | If using Unity Catalog set to catalog to be used. You can omit `ConnCatalog` if not using Unity Catalog. |
# MAGIC | `SCHEMA`         | This is the database for the libname. |
# MAGIC
# MAGIC **NOTE**: While SAS/ACCESS for JDBC works well for most non-distributed reads from Delta, you will want to consider using caslib for large data reads and distributed transforms in SAS. SEE BELOW: **JDBC CAS Connector**.
# MAGIC
# MAGIC **NOTE**: SAS/ACCESS for JDBC will not perform well for large data writes. This is due to how the batch insert process is executed. SEE BELOW: **JDBC Insert Performance**.
# MAGIC
# MAGIC **NOTE**: None of the Variables above are set in these example since they are all set in SAS Studio Job Execution autoexec statement. You will want to set these in the using the <a href="$../ref/sas_context_config.md" target="_blank">sas_context_config.md</a>.
# MAGIC
# MAGIC **NOTE**: Databricks provides a [JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) available for download and the Databricks specific url string configurations can be found at [Databricks JDBC Driver Config](https://learn.microsoft.com/en-us/azure/databricks/integrations/jdbc-odbc-bi#--jdbc-driver).
# MAGIC
# MAGIC **NOTE**: There is a SAS provided Databricks JDBC Driver by [CData](https://cdn.cdata.com/help/LKH/jdbc/). While it has the convenience of being included in SAS Viya already, it has a limitation that you are curently unable to set a default catalog which makes it slightly more cumbersome to use with unity catalog. It will not be reviewed in this notebook, but an example configuration is provided in <a href="$../sas/02-SAS-ACCESS_JDBC_cdata.sas" target="_blank">02-SAS-ACCESS_JDBC_cdata.sas</a>.
# MAGIC
# MAGIC **NOTE**: There is a pre-requisite task to load the JDBC driver into SAS. Those instructions can be found in <a href="$../ref/JDBC_config.md" target="_blank">JDBC_config.md</a>.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.jdbc_cars;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC libname jdbc_dbr clear;
# MAGIC libname jdbc_dbr JDBC 
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="/export/sas-viya/data/drivers/"
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443;transportMode=http;ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;ConnCatalog=sas_interop;ConnSchema=demo;"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;
# MAGIC
# MAGIC data jdbc_dbr.jdbc_cars;
# MAGIC set sashelp.cars;
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sas_interop.demo.jdbc_cars;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## JDBC Insert Performance
# MAGIC
# MAGIC Not seen in the configuration, the JDBC LIBNAME Option [BULKLOAD](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n0j1e5bcfyjygxn1av4lu5yshd3s.htm) defaults to True. This configuration runs the following when executing an insert:
# MAGIC  - Breaks up records to be inserted into batches. 
# MAGIC  - Writes those batches run multiple insert statements into a temp table similar to below (which can be seen in [compute](https://adb-8590162618558854.14.azuredatabricks.net/?o=8590162618558854#setting/clusters) -> cluster -> spark UI -> JDBC/ODBC Server):
# MAGIC ```sql
# MAGIC INSERT INTO TABLE `demo`.`sastmp_181833_513_0000`
# MAGIC        (`Make`, `Model`, `Type`, `Origin`, `DriveTrain`, `MSRP`, `Invoice`, `EngineSize`, `Cylinders`, `Horsepower`, `MPG_City`, `MPG_Highway`, `Weight`, `Wheelbase`, `Length`)
# MAGIC VALUES ('Volvo', ' S60 T5 4dr', 'Sedan', 'Europe', 'Front', 34845, 32902, 2.2999999999999998, 5, 247, 20, 28, 3766, 107, 180),
# MAGIC        ('Volvo', ' S60 R 4dr', 'Sedan', 'Europe', 'All', 37560, 35382, 2.5, 5, 300, 18, 25, 3571, 107, 181), 
# MAGIC        ...
# MAGIC        ('Volvo', ' S80 2.9 4dr', 'Sedan', 'Europe', 'Front', 37730, 35542, 2.8999999999999999, 6, 208, 20, 28, 3576, 110, 190)
# MAGIC ```
# MAGIC  - Then there is a final insert statement that will write all of the inserted records in the temp table into the target table using syntax similar to:
# MAGIC ```sql
# MAGIC INSERT INTO TABLE `demo`.`JDBC_CARS` 
# MAGIC SELECT `Make`, `Model`, `Type`, `Origin`, `DriveTrain`, `MSRP`, `Invoice`, `EngineSize`, `Cylinders`, `Horsepower`, `MPG_City`, `MPG_Highway`, `Weight`, `Wheelbase`, `Length`
# MAGIC FROM `demo`.`sastmp_181833_513_0000`
# MAGIC ```
# MAGIC While it isn't explicitly stated in documentation why there is the stutter step of using the temp table, it appears that it is used to ensure atomicity to the insert statement. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SAS/ACCESS LIBNAME Statement Vs Pass-Through Facility
# MAGIC
# MAGIC Both the *SAS/ACCESS Pass-Through facility* and the *LIBNAME statement* in SAS/ACCESS are used to access data in external databases. However, there are some key differences between the two:
# MAGIC  * **Language**: The Pass-Through facility allows you to use Databricks [Spark SQL](https://www.databricks.com/glossary/what-is-spark-sql#:~:text=Spark%20SQL%20is%20a%20Spark,on%20existing%20deployments%20and%20data.) commands to interact with a Databricks Cluster or SQL Warehouse, while the LIBNAME statement allows you to use standardized SAS Syntax to interface with a Databricks Cluster or SQL Warehouse.
# MAGIC  * **Processing Location**: With the Pass-Through facility, the SQL commands are processed on Databricks, while with LIBNAME, SAS reads the source data and processes transforms within SAS.
# MAGIC  * **Functionality**:  The LIBNAME statement does provide a simpler interface to Databricks, but doesn't take advantage of all of Databricks Capabilities. The Pass-Through facility allows for the full range of capabilities of the external database, including:
# MAGIC     * Complex SQL Functions
# MAGIC     * Grant Operations
# MAGIC  * **Performance**: The Pass-Through facility can provide faster performance for large or complex SQL statements because the SQL commands are processed in Databricks, and only the results are returned to SAS. In contrast, the LIBNAME statement may require more processing on the SAS server, which can affect performance.
# MAGIC
# MAGIC In summary, the SAS/ACCESS Pass-Through facility and the LIBNAME statement are both useful for accessing data in external databases, but they have different strengths and use cases. The Pass-Through facility provides greater flexibility and performance for complex Spark SQL commands, while the LIBNAME statement provides a simpler interface and may be more appropriate for simpler queries or data manipulation tasks.
# MAGIC
# MAGIC To demonstrate a use of each, we will run a simple pivot on the `jdbc_cars` data. We can see that all records (although a subet of columns were returned) for **PROC Tablulate**:
# MAGIC ```sql
# MAGIC SELECT  `JDBC_CARS`.`DriveTrain`, `JDBC_CARS`.`Type`, `JDBC_CARS`.`Horsepower`  FROM `demo`.`JDBC_CARS`
# MAGIC ```
# MAGIC **NOTE**: You may not always see this query in spark ui if the table is already cached in SAS and doesn't need to be read again.

# COMMAND ----------

# MAGIC %%SAS lst
# MAGIC
# MAGIC /* Run a PIVOT using proc tabulate */
# MAGIC
# MAGIC proc tabulate data=jdbc_dbr.jdbc_cars;
# MAGIC class drivetrain type;
# MAGIC var horsepower;
# MAGIC tables type,drivetrain*horsepower*(mean);
# MAGIC run;
# MAGIC
# MAGIC /* Run a PIVOT using Passthrough */
# MAGIC
# MAGIC proc sql; 
# MAGIC connect using jdbc_dbr; 
# MAGIC select * from connection to jdbc_dbr (
# MAGIC     SELECT type,  all_hp, front_hp, rear_hp
# MAGIC     FROM (SELECT type, drivetrain, horsepower FROM jdbc_cars)
# MAGIC     PIVOT (ROUND(AVG(horsepower),2) AS horsepower
# MAGIC            FOR drivetrain
# MAGIC            IN ("Front" AS front_hp, "All" AS all_hp, "Rear" AS rear_hp)))
# MAGIC     ORDER BY type;     
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## JDBC CAS Connector
# MAGIC
# MAGIC SAS CAS (Cloud Analytic Services) is the in-memory, distributed processing engine provided by SAS. It is important to be aware of the connector so that you can more optimially populate a tables in your CAS Session. The databse in our case is our Databricks Interactive Cluster or SQL Warehouse endpoint.
# MAGIC
# MAGIC | Description | Diagram |
# MAGIC | --- | --- |
# MAGIC | **Access Data with a SAS/ACCESS Engine** </br></br> When you use LIBNAME locally, you are actually reading the data into the SAS Compute (locally), </br>but then you would have to move that into distributed form. This extra step can be costly for large </br> data sets.  | ![SAS JDBC DIagram](https://go.documentation.sas.com/api/docsets/casref/v_002/content/images/access.png?locale=en) | 
# MAGIC | **Access Data with a Data Connector or Data Connect Accelerator** </br></br> This allows the data to be read directly into the distributed form (CAS). You can additionally </br> provide configurations that will allow for multiple connections and multiple concurrent reads. | ![SAS JDBC DIagram](https://go.documentation.sas.com/api/docsets/casref/v_002/content/images/dbms.png?locale=en) | 
# MAGIC
# MAGIC When creating a [JDBC CAS Connector](https://documentation.sas.com/doc/en/pgmsascdc/v_035/casref/n1ldk5vubre9oen10bdqoqkfc1y7.htm) for Databricks you will use the following pattern:
# MAGIC ```SAS
# MAGIC caslib jdcaslib dataSource=(srctype='jdbc',
# MAGIC            url="jdbc:databricks://&DBR_HOST:443;
# MAGIC                 transportMode=http;ssl=1;httpPath=&CLUSTER_PATH;
# MAGIC                 AuthMech=3;UID=token;PWD=&DBR_TOKEN;
# MAGIC                 ConnCatalog=sas_interop;ConnSchema=demo;"
# MAGIC            class="com.databricks.client.jdbc.Driver",
# MAGIC            classpath="/access-clients/jdbc");
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC | Variable         | Definition |
# MAGIC | ---------------- | ----------------- |
# MAGIC | `CLASS_PATH`     | The folder path where the [Datbaricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download) is saved.  |
# MAGIC | `DBR_HOST`       | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> JDBC URL. </br>It is also found in the URL of the workspace.                           |
# MAGIC | `HTTP_PATH`      | For clusters: [Compute](#setting/clusters) -> Cluster -> Advanced Options -> HTTP path. </br> For warehouses: [SQL Warehouses](./sql/warehouses) -> Warehouse -> Connection Details -> HTTP path. |
# MAGIC | `DBR_TOKEN`      | This is your [Databricks Personal Access Token](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/authentication) which is found in [User Settings](#setting/account/token) -> Access Tokens |
# MAGIC | `ConnCatalog`    | If using Unity Catalog set to catalog to be used. You can omit `ConnCatalog` if not using Unity Catalog. |
# MAGIC | `ConnSchema`     | This is the database for the libname. |
# MAGIC
# MAGIC
# MAGIC **NOTE**: While reading for use in CAS is useful, there is an exception created when writing tables that makes use of this caslib unsuitable for saving data. SEE BELOW: **Table Save Exception**. However, even if this were to work, we wouldn't advise it because the approach uses INSERT INTO VALUES statements that are inefficient. Thus, if you need to write bulk data into Databricsk readible format look to saving the data directly into cloud storage.
# MAGIC
# MAGIC **NOTE**: The SAS provided Databricks JDBC Driver by [CData](https://cdn.cdata.com/help/LKH/jdbc/) can also be configured and does not have an exception message when used. However, it has a limitation that it can only save to `hive_metastore` and uses INSERT INTO VALUES statments which are inefficient. Thus, if you need to write bulk data into Databricsk readible format look to saving the data directly into cloud storage. Since this could still be an option if save to blob storage isn't viable, reference code is provided in  <a href="$../sas/02-SAS-ACCESS_JDBC_cdata.sas" target="_blank">02-SAS-ACCESS_JDBC_cdata.sas</a>.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC CAS mySession  SESSOPTS=( CASLIB=casuser TIMEOUT=99 LOCALE="en_US" metrics=true);
# MAGIC
# MAGIC caslib jdcaslib dataSource=(srctype='jdbc',
# MAGIC            url="jdbc:databricks://&DBR_HOST:443;transportMode=http;ssl=1;httpPath=&CLUSTER_PATH;AuthMech=3;UID=token;PWD=&DBR_TOKEN;ConnCatalog=sas_interop;ConnSchema=demo;",
# MAGIC            class="com.databricks.client.jdbc.Driver",
# MAGIC            classpath="/export/sas-viya/data/drivers/");
# MAGIC
# MAGIC proc casutil;
# MAGIC    contents casdata="jdbc_cars";
# MAGIC run; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here, we are able to see the `jdbc_cars` table we created above. However, we are going to want create a table based upon that one in CAS.
# MAGIC
# MAGIC When working with CAS Datasources, you need to run data transforms for cas in deticated distributed procs. Proc SQL becomes [PROC FEDSQL](https://documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n07usx4t0lq8tfn1rbt7jz2eb1m2.htm). This is how we can create and then inspect tables.
# MAGIC
# MAGIC **NOTE**: Be minful that if you are running distributed queries the tables are quite large and therefor large select queries should be avoided due to performance implications to return the results to the SAS Compute session. Consider putting a limit on your queries.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC proc fedsql sessref=mySession;
# MAGIC   create table cascars {options replace=true} as
# MAGIC   SELECT * FROM jdbc_cars;
# MAGIC quit;
# MAGIC
# MAGIC proc casutil;
# MAGIC    contents casdata="cascars";
# MAGIC run;
# MAGIC
# MAGIC proc fedsql sessref=mySession;
# MAGIC   SELECT * FROM cascars LIMIT 5;
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since we are in a distributed context when using CAS, we also have distributed concepts like partitioning.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC proc cas;
# MAGIC table.partition /                                                   
# MAGIC casout={caslib="jdcaslib", name="cascars_part"}                             
# MAGIC table={caslib="jdcaslib", name="cascars", groupby={name="type"}};   
# MAGIC run; 
# MAGIC quit;
# MAGIC
# MAGIC proc casutil;
# MAGIC    contents casdata="cascars_part";
# MAGIC run; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## JDBC CAS Connector Table Save Exception
# MAGIC
# MAGIC When trying to save our table we run into a create table exception from Databricks JDBC/ODBC Server. This is due to invalid syntax being generated during the save process. While this is an unfortunate loss of functionality, we likely would not use this to save data anyways due to poor performance. You can run the code below to see the exception from SAS and you can inspect the invalida query submitted by going to [Compute](#setting/clusters) -> Cluster -> Spark UI -> JDBC/ODBC Server.

# COMMAND ----------

# MAGIC %%SAS log
# MAGIC proc casutil incaslib="jdcaslib"; 
# MAGIC    save casdata="cascars_part" outcaslib="jdcaslib" ; 
# MAGIC run;
