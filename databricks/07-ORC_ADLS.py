# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ORC Table using ADLS `FILENAME <table> orc`
# MAGIC
# MAGIC Within an [ADLS FILENAME Statement](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) we are able to assign an [ORC Engine](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/p1aq5w1grouaodn1pxwuvt9xy88z.htm). This will allow us to have a medium format that is performant in both SAS and Databricks and can be read in either platform with need for running concurrent sessions. This notebook will go through the following useful patterns when working with ORC Tables:
# MAGIC
# MAGIC  * Multiple DATA-files per folder
# MAGIC  * Single DATA-file per folder
# MAGIC  * Single DATA-file per sub-folders
# MAGIC  * Multiple DATA-PART-files per folder
# MAGIC
# MAGIC The SAS Code in this demo is also available as SAS pure code in []().

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Multiple DATA-files per folder
# MAGIC
# MAGIC This concept is pretty straight forward in SAS. There is a deticated folder where all the files in the folder are a table persisted as an ORC file. In SAS, the convention is that each file name is `<table_name>.orc` and there is a libname that contains all the tables. We'll write out `cars` and `class` in this example. Since these two data sources don't share the same schema, it doesn't make since to create a table definition on a folder containing these files. However, we can still access each of these files directly in Databricks using [read_orc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_orc.html#pyspark-pandas-read-orc).
# MAGIC
# MAGIC **NOTE**: We can and will create a table, `cars_orc_mixd`, from the `cars.orc` file, but it is more common in distributed environments to deticate a single schema to the entire folder instead of having mixed schemas within the same folder.
# MAGIC
# MAGIC **NOTE**: You typically will also set `azuretenantid` option, but we've done this in the context configuration for brevity in the examples.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Multiple DATA-files per folder */
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC
# MAGIC libname orc_mixd orc 'external/demo/orc_mixd'
# MAGIC    storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC    storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC    storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC    directories_as_data=no;
# MAGIC
# MAGIC data orc_mixd.cars;
# MAGIC set sashelp.cars;
# MAGIC run;
# MAGIC
# MAGIC data orc_mixd.class;
# MAGIC set sashelp.class;
# MAGIC run;
# MAGIC
# MAGIC proc contents data=orc_mixd._all_ nods;
# MAGIC run;

# COMMAND ----------

display(dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_mixd'))

# COMMAND ----------

from pyspark import pandas as pd
class_df = pd.read_orc('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_mixd/class.orc')
display(class_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can now put a schema on the external location file and read the table directly in Databricks.
# MAGIC -- NOTICE that the location is a file
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.orc_cars_mixd;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.orc_cars_mixd (
# MAGIC     Make        STRING,
# MAGIC     Model       STRING,
# MAGIC     Type        STRING,
# MAGIC     Origin      STRING,
# MAGIC     DriveTrain  STRING,
# MAGIC     MSRP        decimal(8,0),
# MAGIC     Invoice     decimal(8,0),
# MAGIC     EngineSize  double,
# MAGIC     Cylinders   double,
# MAGIC     Horsepower  double,
# MAGIC     MPG_City    double,
# MAGIC     MPG_Highway double,
# MAGIC     Weight      double,
# MAGIC     Wheelbase   double,
# MAGIC     Length      double)
# MAGIC USING ORC
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_mixd/cars.orc';
# MAGIC SELECT * FROM sas_interop.demo.orc_cars_mixd;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC /* We can read this table using JDBC since it has a table defined in Databricks */
# MAGIC
# MAGIC libname jdbc_dbr clear;
# MAGIC
# MAGIC libname jdbc_dbr JDBC 
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="/export/sas-viya/data/drivers/"
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
# MAGIC         ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;
# MAGIC         ConnCatalog=sas_interop;
# MAGIC         ConnSchema=demo;"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC     select * from  orc_mixd.cars;
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Single DATA-file per folder
# MAGIC
# MAGIC We can use the same FILENAME `directories_as_data=no` as before, but instead this time, by convention, the developer will only write a single file into a directory. The intent being if there is a single file, there is a single schema and you can set the entire directory as a table in Databricks - thus mapping one table to one directory.
# MAGIC
# MAGIC We can still create a table definition usin the [Create Table \[using\] Syntax](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using#syntax).
# MAGIC
# MAGIC We can then read all these one table, one directory tables using FILENAME `directories_as_data=yes`. However there are some limitaions to this:
# MAGIC
# MAGIC  * You are unable to use FILENAME `directories_as_data=yes` to read directories that have more than one file - we'll show this later in the **Multiple DATA-PART-files per folder** section
# MAGIC  * You have mixed use on how filename is applied; a single read for all directories and multiple writes libnames for each file
# MAGIC
# MAGIC Ultimately, the need to have a separate write for a single directory, but then have a read for multiple directories makes this appraoch awkward and **not recommended**. Consider instead to read and write directories using CAS or read and write files within a directory using FILENAME `directories_as_data=no`.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Notice that the libname has the table name, cars in it, this is not typical, but needed since each table we write will need a deticated libname */
# MAGIC
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC libname orc_cars orc 'external/demo/orc_dad/cars'
# MAGIC    storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC    storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC    storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC    directories_as_data=no;
# MAGIC
# MAGIC data orc_cars.cars;
# MAGIC set sashelp.cars;
# MAGIC run;
# MAGIC
# MAGIC /* Notice that the libname has the table name, cls in it, this is not typical, but needed since each table we write will need a deticated libname */
# MAGIC
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC libname orc_cls orc 'external/demo/orc_dad/class'
# MAGIC    storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC    storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC    storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC    directories_as_data=no;
# MAGIC
# MAGIC data orc_cls.class;
# MAGIC set sashelp.class;
# MAGIC run;

# COMMAND ----------

# We can verify that this data was just written by inspecting the directory location (https://www.epochconverter.com/):
display(dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_dad/cars') +
        dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_dad/class'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can now put a schema on the external location and read the table directly in Databricks.
# MAGIC -- NOTEICE that the LOCATION is a folder
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.orc_cars_dad;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.orc_cars_dad (
# MAGIC     Make        STRING,
# MAGIC     Model       STRING,
# MAGIC     Type        STRING,
# MAGIC     Origin      STRING,
# MAGIC     DriveTrain  STRING,
# MAGIC     MSRP        decimal(8,0),
# MAGIC     Invoice     decimal(8,0),
# MAGIC     EngineSize  double,
# MAGIC     Cylinders   double,
# MAGIC     Horsepower  double,
# MAGIC     MPG_City    double,
# MAGIC     MPG_Highway double,
# MAGIC     Weight      double,
# MAGIC     Wheelbase   double,
# MAGIC     Length      double)
# MAGIC USING ORC
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_dad/cars';
# MAGIC SELECT * FROM sas_interop.demo.orc_cars_dad;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can now put a schema on the external location and read the table directly in Databricks.
# MAGIC -- NOTEICE that the LOCATION is a folder
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.orc_class_dad;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.orc_class_dad (
# MAGIC     name    STRING,
# MAGIC     sex     STRING,
# MAGIC     age     DOUBLE,
# MAGIC     height  DOUBLE,
# MAGIC     weight  DOUBLE)
# MAGIC USING ORC
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_dad/class';
# MAGIC SELECT * FROM sas_interop.demo.orc_class_dad;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC /* We can use the libname to read folders as tables, but we cannot write, nor read multiple files */
# MAGIC
# MAGIC libname orc_dad orc 'external/demo/orc_dad'
# MAGIC    storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC    storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC    storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC    directories_as_data=yes;
# MAGIC
# MAGIC proc contents data=orc_dad._all_ nods;
# MAGIC run;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from orc_dad.cars;
# MAGIC quit;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from orc_dad.class;
# MAGIC quit;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN sas_interop.demo

# COMMAND ----------

# MAGIC %%SAS
# MAGIC /* As expected we can still read this table in SAS using JSBC */
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from jdbc_dbr.orc_class_dad;
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Single DATA-file per sub-folders (Partitioned ORC Table)
# MAGIC
# MAGIC Many times, there will be a need for processing data daily in SAS and should be partitined by date. Other times, the data itself naturally lends itself to being [partitioned](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-partition) for performance reasons. Here we will run the same demo as above except this time we will write into a partitioned ORC table.
# MAGIC
# MAGIC **NOTE**: For this example to make sense, you have to be aware that the [Hive ORC File](https://cwiki.apache.org/confluence/display/hive/languagemanual+orc) convention for a partitioned columns

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC %macro write_carType(carType);
# MAGIC
# MAGIC libname adls_orc orc "external/demo/cars_orc_part/type=&carType"
# MAGIC     storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC     storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC     storage_file_system = "&ADLS_FILESYSTEM";
# MAGIC
# MAGIC data adls_orc.orc_cars;                            
# MAGIC     set sashelp.cars;                             
# MAGIC     where Type="&carType";
# MAGIC run;
# MAGIC %mend write_carType;
# MAGIC
# MAGIC %write_carType(Sports)
# MAGIC %write_carType(SUV)
# MAGIC %write_carType(Sedan)
# MAGIC %write_carType(Hybrid)
# MAGIC %write_carType(Truck)
# MAGIC %write_carType(Wagon)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can now put a partitioned schema on the external location and read the table directly in Databricks.
# MAGIC -- Notice how we've added the partition by type
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.cars_orc_part;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.cars_orc_part (
# MAGIC     Make        STRING,
# MAGIC     Model       STRING,
# MAGIC     Origin      STRING,
# MAGIC     DriveTrain  STRING,
# MAGIC     MSRP        decimal(8,0),
# MAGIC     Invoice     decimal(8,0),
# MAGIC     EngineSize  double,
# MAGIC     Cylinders   double,
# MAGIC     Horsepower  double,
# MAGIC     MPG_City    double,
# MAGIC     MPG_Highway double,
# MAGIC     Weight      double,
# MAGIC     Wheelbase   double,
# MAGIC     Length      double)
# MAGIC USING ORC
# MAGIC PARTITIONED BY (Type STRING)
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/cars_orc_part';
# MAGIC SELECT * FROM sas_interop.demo.cars_orc_part;

# COMMAND ----------

# We can verify that partition data was just written by inspecting the directory location (https://www.epochconverter.com/):
display(dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/cars_orc_part/'))

# COMMAND ----------

# MAGIC %%SAS
# MAGIC /* We can even read the partitioned ORC Table using our existing SAS/ACCESS JDBC, ODBC, and SPARK methods */
# MAGIC libname jdbc_dbr JDBC 
# MAGIC    driverclass="com.databricks.client.jdbc.Driver"
# MAGIC    classpath="/export/sas-viya/data/drivers/"
# MAGIC    URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
# MAGIC         ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;
# MAGIC         ConnCatalog=sas_interop;
# MAGIC         ConnSchema=demo;"
# MAGIC    user="token" 
# MAGIC    password=&DBR_TOKEN;
# MAGIC
# MAGIC /* TODO: include jdbc_dbr in SAS Launcher context so we dont have to write out libname */
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from jdbc_dbr.cars_orc_part;
# MAGIC quit;

# COMMAND ----------

# MAGIC %%SAS lst log
# MAGIC
# MAGIC /* We can not read this as a partitioned table directly in SAS, each partition is treated as an awkwardly named table */
# MAGIC
# MAGIC libname orc_part orc 'external/demo/cars_orc_part/'
# MAGIC    storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC    storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC    storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC    directories_as_data=yes;
# MAGIC
# MAGIC proc contents data=orc_part._all_ nods;
# MAGIC run;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from orc_part.type=Hybrid;
# MAGIC quit;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ORC Tables LIBNAME
# MAGIC
# MAGIC Notice, that we are using the JDBC connection to access the data during the validation data can be read in SAS. This is actually not ideal in all cases becuase it requires that there is a running Databricks session which is unnecessary compute. We are instead able to read the data back as a libname for all ORC tables. This feature will make it so that we will want to organize all ORC tables together in our external location path so that we can have a single accessible libname for all our ORC tables. For this demo we will call that libname `adls_orc`.
# MAGIC
# MAGIC **NOTE**: There are muliple [restrictions](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/n0lbcs22nfb68jn1689ey9y9aujk.htm) for ORC tables including:
# MAGIC
# MAGIC  * Compound data types are not supported.
# MAGIC  * **Hive data partitioning is not supported. (SAS does support tables that consist of multiple files.)** - thus, this option will work for `cars_orc`, but not `cars_orc_part`
# MAGIC  * SAS does not use ORC predicate pushdown for WHERE clause optimization.

# COMMAND ----------

display(dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/cas'))

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC
# MAGIC libname adls_orc orc "external/demo/orc"
# MAGIC     storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC     storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC     storage_file_system = "&ADLS_FILESYSTEM"
# MAGIC     directories_as_data=yes;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from adls_orc.cars_orc;
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Summary ORC File Storage Patterns
# MAGIC
# MAGIC So there are multiple storage patterns that ORC files can be stored as in ADLS so it is worth while to list them here and comment on the interoperability in SAS and Databricks. Additionally provided when/if recommended.
# MAGIC | ADLS Pattern                           | SAS Interoperability                                              | Databricks Interoperability | Recommended |
# MAGIC | -------------------------------------- | ----------------------------------------------------------------- | --------------------------- | ----------- |
# MAGIC | Multiple DATA-files per folder | Expected behavior for LIBNAME `directories_as_data=no` | Each file can have a different schema, use [pyarrow.orc](https://arrow.apache.org/docs/python/generated/pyarrow.orc.ORCFile.html) | **iff** primary use is in SAS and available to Databricks |
# MAGIC | Single DATA-file per folder | Expected behavior for LIBNAME `directories_as_data=yes` | Read as non-partitioned ORC Table | No, and SAS fails if multiple files in folders. |
# MAGIC | Single DATA-file per sub-folders | Able to write, not read, using LIBNAME `directories_as_data=no` | Read as partitioned ORC Table  | **iff** logically partitioned by SAS job run |
# MAGIC | Multiple DATA-PART-files per folder | This is expected behavior for DataSet | Read as non-partitioned ORC Table  | Yes |
# MAGIC | Multiple DATA-PART-files per sub-folders | This can be written, not read, using DataSet | Read as partitioned ORC Table  | **iff** logically partitioned by SAS job run |

# COMMAND ----------


