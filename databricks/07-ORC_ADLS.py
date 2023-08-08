# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ORC Table using ADLS `FILENAME <table> orc`
# MAGIC
# MAGIC Within an [ADLS FILENAME Statement](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) we are able to assign an [ORC Engine](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/p1aq5w1grouaodn1pxwuvt9xy88z.htm). This will allow us to have a storage format that is performant in both SAS and Databricks and can be read in either platform without the need for running concurrent sessions. This notebook will go through the following useful patterns when working with ORC Tables:
# MAGIC
# MAGIC  * Multiple Data Source files with different schemas in single folder
# MAGIC  * Single Data Source file per subfolder (separate read / write libnames)
# MAGIC  * Single Data Source Schema in Sub-Folders (Partitioned ORC Table)
# MAGIC  * Multiple Data Source files with same schemas in single folder (Non-Partitioned ORC Table)
# MAGIC
# MAGIC The SAS Code in this demo is also available as SAS pure code in <a href="$../sas/07-ORC_ADLS.sas" target="_blank">07-ORC_ADLS.sas</a>.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Multiple Data Source files with different schemas in single folder
# MAGIC
# MAGIC This concept is pretty straight forward in SAS. There is a deticated folder where all the files in the folder are a separate data sets persisted as ORC files. In SAS, the convention is that each file name is `<table_name>.orc` and there is a libname that contains all the tables. We'll write out `cars` and `class` in this example. Since these two data sources don't share the same schema, it doesn't make since to create a Databricks table definition on a folder containing these files. However, we can still access each of these files directly in Databricks using [read_orc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_orc.html#pyspark-pandas-read-orc).
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/orc_multi-file_diff.png" alt="orc_multi-file_diff" width="600px">
# MAGIC
# MAGIC
# MAGIC **NOTE**: We can and will create a table, `cars_orc_mixd`, from the `cars.orc` file, but it is more common in distributed environments to deticate a single schema to the entire folder instead of having mixed schemas within the same folder.
# MAGIC
# MAGIC **NOTE**: You see we set `azuretenantid` option, but could also have been done in the context configuration if only working with a single azure tenant.

# COMMAND ----------

# MAGIC %%SAS lst
# MAGIC
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
# We are able to read the file directly using read_orc
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
# MAGIC ## Single Data Source file per subfolder (separate read / write libnames)
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
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/orc_single_file_subfolders.png" alt="orc_single_file_subfolders" width="600px">
# MAGIC
# MAGIC This structure has the advantage that we can read all tables through a single orc libname. Ultimately, the need to have a separate write for a single directory, but then have a read for multiple directories makes this appraoch awkward and **not recommended**.

# COMMAND ----------

# MAGIC %%SAS
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
# MAGIC -- NOTICE that the LOCATION is a folder
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

# MAGIC %md
# MAGIC
# MAGIC ## Single Data Source Schema in Sub-Folders (Partitioned ORC Table)
# MAGIC
# MAGIC Many times, there will be a need for processing data daily in SAS and should be partitined by date. Other times, the data itself naturally lends itself to being [partitioned](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-partition) for performance reasons. Here we will run the same demo as above except this time we will write into a partitioned ORC table.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/orc_partitioned_table.png" alt="orc_partitioned_table" width="600px">
# MAGIC
# MAGIC **NOTE**: For this example to make sense, you have to be aware that the [Hive ORC File](https://cwiki.apache.org/confluence/display/hive/languagemanual+orc) convention for a partitioned columns.
# MAGIC
# MAGIC **NOTE**: The partitioned table approach could be modified so that each partition folder has a unique name that excludes `=` has only a single orc file in the subfolder. In this case you could also create a libname and read aech of the partitions by it's partition folder name. However, again, you would not be able to write to the partitions with this libref. This approach would require the use of [ALTER TABLE ADD PARTITION](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-alter-table.html#syntax) not shown here.
# MAGIC
# MAGIC **NOTE**: Since we already are not able to read, we could write multiple same schema files to a single subfolder. These files can use a single libref per subfolder. The example below shows only a single file written per partition folder, an example with muliple data sources with same schema in the next section.

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

# MAGIC %sql
# MAGIC DESCRIBE formatted sas_interop.demo.cars_orc_part;

# COMMAND ----------

# We can verify that partition data was just written by inspecting the directory location (https://www.epochconverter.com/):
display(dbutils.fs.ls('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/cars_orc_part/'))

# COMMAND ----------

# MAGIC %%SAS
# MAGIC /* We can even read the partitioned ORC Table using our existing SAS/ACCESS JDBC, ODBC, and SPARK methods */
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

# MAGIC %md
# MAGIC
# MAGIC ## Multiple Data Source files with same schemas in single folder (Non-Partitioned ORC Table)
# MAGIC
# MAGIC In hadoop environments the most common pattern is to have many files of the same schema saved in a single folder. While read such a folder isn't the expected behavior in SAS, it is the expected behavior in Spark. We can acomplish writing such a directory by creating multiple data source and setting `directories_as_data=no`. The watchout here is that each file (thus spark dataframe partion on read) will be a separate datasource in SAS.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/orc_multi-file_same.png" alt="orc_partitioned_table" width="600px">
# MAGIC
# MAGIC
# MAGIC **NOTE**: This concept can be applied to the partitioned table in the above section, but will be explicitly shown here.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC libname orc_np orc "external/demo/orc_cars_nonpart"
# MAGIC     storage_account_name = "&ADLS_ACCOUNT_NAME"
# MAGIC     storage_application_id = "&ADLS_APPLICATION_ID"
# MAGIC     storage_file_system = "&ADLS_FILESYSTEM";
# MAGIC
# MAGIC %macro write_carDriveTrain(carDriveTrain);
# MAGIC data orc_np.cars_&carDriveTrain;                            
# MAGIC     set sashelp.cars;                             
# MAGIC     where DriveTrain="&carDriveTrain";
# MAGIC run;
# MAGIC %mend write_carDriveTrain;
# MAGIC
# MAGIC %write_carDriveTrain(Front)
# MAGIC %write_carDriveTrain(All)
# MAGIC %write_carDriveTrain(Rear)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can now put a non-partitioned schema on the external location and read the table directly in Databricks.
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.orc_cars_nonpart;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.orc_cars_nonpart (
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
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/orc_cars_nonpart';
# MAGIC SELECT * FROM sas_interop.demo.orc_cars_nonpart;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT input_file_name(), DriveTrain, count(*) cnt FROM sas_interop.demo.orc_cars_nonpart GROUP BY 1, 2;

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC proc contents data=orc_np._all_ nods;
# MAGIC run;
# MAGIC
# MAGIC proc sql outobs=5;
# MAGIC select * from orc_np.cars_Rear;
# MAGIC quit;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Summary of Advantage and Restrictions of ORC Tables LIBNAME
# MAGIC
# MAGIC Notice, that we are using the JDBC connection to access the data during the validation data can be read in SAS. This is actually not ideal in all cases becuase it requires that there is a running Databricks session which is potentially unnecessary compute. We are instead able to read the data back as a libname for all ORC tables. Since the concept of data source (tables) as files isn't consistant with hive (Databricks) default behavior of folder as tables, we have to be mindful how our folder structure changes will impact configurations in SAS and Databricks. The following are useful [restrictions](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/n0lbcs22nfb68jn1689ey9y9aujk.htm)  to be aware of:
# MAGIC
# MAGIC  * Compound data types are not supported.
# MAGIC  * **Hive data partitioning is not supported. (SAS does support tables that consist of multiple files.)**
# MAGIC  * SAS does not use ORC predicate pushdown for WHERE clause optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ORC CAS
# MAGIC
# MAGIC Below is an example of the [ORC Data Connector](https://documentation.sas.com/doc/en/pgmsascdc/v_035/casref/p13fqwnolibluhn14ajoe5wc462l.htm) doesn't allow for [parallel write](https://communities.sas.com/t5/SAS-Communities-Library/SAS-Viya-3-5-SAS-ORC-LIBNAME-engine-to-ADLS2/ta-p/653589) so there doesn't appear to be a strong use case for writing from CAS to ADLS ORC folder over using just a libref.
# MAGIC

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Create a CAS session with caslib with cars */
# MAGIC CAS mySession
# MAGIC     SESSOPTS=(azureTenantId="&AZ_TENANT_ID");
# MAGIC
# MAGIC proc cas;
# MAGIC   session mySession;
# MAGIC   addcaslib /
# MAGIC   datasource={srctype="adls"
# MAGIC                accountName="&ADLS_ACCOUNT_NAME"
# MAGIC                filesystem="&ADLS_FILESYSTEM"
# MAGIC                applicationId="&ADLS_APPLICATION_ID"
# MAGIC               }
# MAGIC    name="adlscas"
# MAGIC    subdirs=true
# MAGIC    path="external/demo/orc_cas_large";
# MAGIC run;
# MAGIC quit;
# MAGIC
# MAGIC /* This will assign a libname to adlscas */
# MAGIC CASLIB _ALL_ ASSIGN SESSREF=mySession;
# MAGIC * libname adlscas cas sessref=mySession;
# MAGIC
# MAGIC /* We now have an in-memory cars table in adlscas */
# MAGIC /* not used, just to demo to memory table from other libnames*/
# MAGIC data adlscas.cars;            
# MAGIC    set sashelp.cars;
# MAGIC run;
# MAGIC
# MAGIC /* Create a large in-memory table */
# MAGIC data adlscas.large;
# MAGIC array vars(300) $8 x1-x300;
# MAGIC do j=1 to 500000;
# MAGIC id=put(rand('integer',1,4),8.);
# MAGIC do i=1 to 300;
# MAGIC vars(i)=rand("Uniform");
# MAGIC end;
# MAGIC output;
# MAGIC end;
# MAGIC drop i j;
# MAGIC run;
# MAGIC
# MAGIC
# MAGIC %macro save_largePart(carPart);
# MAGIC proc fedsql SESSREF=mySession;
# MAGIC     CREATE TABLE adlscas.large_&&carPart
# MAGIC     AS SELECT * FROM adlscas.large
# MAGIC     WHERE id = &carPart;
# MAGIC quit;
# MAGIC proc cas;
# MAGIC table.save /
# MAGIC    caslib="adlscas"
# MAGIC    table="large_&&carPart"
# MAGIC    name="large_&&carPart"
# MAGIC    replace=True
# MAGIC    exportoptions={filetype="orc"};
# MAGIC run;
# MAGIC quit;
# MAGIC %mend save_largePart;
# MAGIC
# MAGIC %MACRO save_loop;
# MAGIC %DO p = 1 %TO 4;
# MAGIC  %save_largePart(&p);
# MAGIC %END;
# MAGIC %mend save_loop;
# MAGIC %save_loop
