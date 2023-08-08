# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SAS In-Database Features
# MAGIC
# MAGIC SAS Viya offers a lot of in-database features. One helpful way to keep straing which features are available to Databricks is to list them all out with links to documentation which can save effort on discovery if an in-database feature is supported for Databricks.
# MAGIC
# MAGIC | SAS In-Database Feature | Supported Interfaces | SAS In-Database Technologies </br> Deployment Required | Limitations |
# MAGIC | ----------------------- | -------------------- |:------------------------------------------------------:| ----------- |
# MAGIC | SQL Passthrough Facility </br> (connect to ... syntax)| [JDBC]() </br> [ODBC]() </br> [Spark]() | NO | ODBC untested; </br > - Unable to connect with [Databricks ODBC](https://www.databricks.com/spark/odbc-drivers-download) Driver, SEE </br> - do not have license to [CData ODBC](https://cdn.cdata.com/help/LKH/odbc/default.htm) Driver   |
# MAGIC | PROC FEDSQL </br> (Non-Parallel Transfer to CAS) | [JDBC]() </br> [ODBC]() </br> [Spark]() | NO | [CData](https://www.cdata.com/drivers/databricks/docs/) Drivers do not have default UC Catalog setting </br> [Databricks JDBC](https://www.databricks.com/spark/jdbc-drivers-download) Driver configurations `ConnCatalog` and `ConnSchema` are not observed. </br> ODBC untested; </br > - Unable to connect with [Databricks ODBC](https://www.databricks.com/spark/odbc-drivers-download) Driver, </br> - do not have license to [CData ODBC](https://cdn.cdata.com/help/LKH/odbc/default.htm) Driver |
# MAGIC | PROC DS2 | **NONE** | NO | Databricks is not in the list of [Supported DS2 Data Sources](https://documentation.sas.com/doc/en/pgmsascdc/v_035/ds2ref/p1plf9l0gzf538n1ixvpcwiyc9gk.htm) |
# MAGIC | Base SAS In-Database Proceedures | **NONE** | NO | There is no [supported interface](https://documentation.sas.com/doc/en/pgmsascdc/v_035/indbug/n1lo1d19ercldkn14vm45k13z3so.htm#n0usq0djt8kncln1ozuk40ktvz4h) for Databricks |
# MAGIC | Parallel Transfer to CAS | **NONE** | **YES** | There is no [supported interface](https://documentation.sas.com/doc/en/pgmsascdc/v_035/indbug/n1lo1d19ercldkn14vm45k13z3so.htm#n0usq0djt8kncln1ozuk40ktvz4h) for Databricks |
# MAGIC | In-Database Model Scoring | [Databricks](https://documentation.sas.com/doc/en/pgmsascdc/v_035/indbug/n0b9botqbf4pp2n1g0dfuwg2123p.htm) | **YES** | Untested, do not have Software Order Email (SOE) for installation |
# MAGIC
# MAGIC **NOTE**: You must [license](https://documentation.sas.com/doc/en/pgmsascdc/v_035/indbug/p1e2l1fuavxcbmn1h1bkbcug7zsn.htm#p0frix8fx0o6fon1biyuorpzr5ot) SAS In-Database for Databricks if it requires deployment. Thus, to use Databricks In-Database Model scoring requires an additional licesnse.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Recommended Uses for SAS In-Database Features
# MAGIC
# MAGIC When there are SAS processes that need the entire table, all records, all columns you are not making use of any In-Database Feature, SAS generates a query to read a table metadata and data in entirety and all filtering, column select is done within the local SAS session. This is fine for small tables or when the entire table will be iterated over many times.
# MAGIC
# MAGIC In-Database processing can come with some costs - specifically, when you have a SAS process that is running and awaiting a In-Database process, there are two environments running, both of which will have costs. However, there are scenarios when it is advised to use the following In-Database features:
# MAGIC
# MAGIC ### SQL Passthrough Facility
# MAGIC
# MAGIC Any combination of the following four scenarios supports the use of Databricks SQL Passthrough:
# MAGIC  - **High cost joins / transformations** - When multiple sources in Databricks need to be joined or there are expensive transform operations we can take advantage of the existing distributed architecture of databricks to execute. This may return fewer tables / records or save the complexity of instantiating a CAS sessions.
# MAGIC  - **Minimize Transfer Data Volume** - Databricks includes many performance improvments like partition and file skipping, which make it very fast at handling filter tasks. Thus instead of reading a very large tables set into SAS, you can use Databricsk to do the needle-in-haystack or subset operation which will reduce the total number of records that has to be transfered to SAS. Additionally, aggregate operations can also significantly reduce the total volume of data that has to be transfered. The efficicency here isn't so much saving processing as it is minimizing the data transfer cost.
# MAGIC  - **Run non-ANSI Spark SQL** - Whenever you want to run Spark SQL which isn't supported ANSI-SQL, using passthrough will allow a SQL statement as if run within Databricks.
# MAGIC  - **Inaccessible / Delta / Managed Table** - SAS can't access files that it doesn't an applicaiton id and permissions to. SAS doesn't have an Interface for Delta. SAS shouldn't (against Databricks best practices) access Managed tables directly. For each of these we are using databricks to simply
# MAGIC  - **Avoiding Catalog constraints** - Since the CData JDBC driver is installed by default in some SAS deployments, it might be used to instead of the Databricks provided JDBC driver which requires additional installation steps. If this is the case, there is a limitation when using the CData driver that you are unable to specify a UC Catalog in your sql statement. However, if you use query passthrough, you are able to use `<catalog>.<schema>.<table>` format for you table. This is also a viable way to call a catalog other than the one specified in `connCatalog` when using the Databricks JDBC Driver.
# MAGIC
# MAGIC ### PROC FEDSQL (Non-Parallel Transfer to CAS)
# MAGIC
# MAGIC Unfortunately, there is no supported process for using the [JDBC](https://documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1usgr00wc9cvln1gnyp1807qu17.htm), [ODBC](https://documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1g72kbb0m01y1n1gm1lh532n5ru.htm), [Spark](https://documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1qzm30adels9wn1oze9mgyc5pwc.htm#p02qrrsrhwkhinn1lze3htbpv58c) Data Connectors to read data in parallel from Databricks to SAS Cloud Analytic Servce (CAS). However, if the SAS process will require CAS and if the location is in the `hive_metastore` catalog, there is some minor efficiency in creating the caslib using the CData driver and then use that to create a table in cas using fedsql. Otherwise, it is best to just use a regular JDBC, ODBC, or Spark libname and data operation to create a CAS table.
# MAGIC
# MAGIC **NOTE**: 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Use Databricks SQL Serverless to Further Reduce Cost
# MAGIC
# MAGIC A primary cost concern when using SAS with In Database features is the risk that two services are running for a single application SAS + Databricks. Many scenarios will reduce compute time in SAS by leveraging SAS In-Database features, but we will also want to minimize spend on Databricks. Thus, to have a Databricks service readily avaialble and only pay for the compute time it is use, I recommend leveraging [Databricks SQL Serverless](https://learn.microsoft.com/en-us/azure/databricks/serverless-compute/#databricks-sql-serverless). Once configured, benefits are straight forward:
# MAGIC  - A SAS developer no longer needs to manage the start and stop of a Databricks cluster or SQL warehouse
# MAGIC  - There is no longer a need to await the start time of the Databricks cluster, serverless provides resources ready for processing
# MAGIC  - There is a cost savings using serversless there is no demand you are not charged for an idle Databricks cluster
