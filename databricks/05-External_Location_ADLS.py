# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC For non-SAS users, it is helpful to take a step back and review SAS [terminology](https://documentation.sas.com/doc/en/pgmsascdc/v_035/sqlproc/p1k607ml73z91an1vqibxcu327i1.htm) for tables:
# MAGIC
# MAGIC | SQL Term | SAS Term | Data Processing Term |
# MAGIC | -------- | -------- | -------------------- |
# MAGIC | table    | SAS data set | file   |
# MAGIC | row      | observation  | record |
# MAGIC | column   | variable     | field  |
# MAGIC
# MAGIC Thus, when talking about SAS interoperability with ADLS, there will be two statements that we will use:
# MAGIC
# MAGIC | SAS STATEMENT | Description |
# MAGIC | ------------- | ----------- |
# MAGIC | [FILENAME](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) | This will create a single reference to a file location that can be used to write directly to a file path in ADLS2. There is no libref created in this instance. </br> With this reference you can write by line input to a text file using a [data step](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsref/p1bp8z934fjg2pn1rjlh9vrqq0iv.htm#n00ebkyjnimfijn15wzyfhzmlsy8) step with [file](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsref/n15o12lpyoe4gfn1y1vcp6xs6966.htm#n1pyebpstm8ukbn1o7wqwrp9n7k9) statement. </br> When used with [PROC EXPORT](https://documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n0ku4pxzx3d2len10ozjgyjbrpl9.htm) this is a viable way to write a csv or comma delimited text files from a SAS data set. |
# MAGIC | [LIBNAME </br> (ORC & Parquet)](https://documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/p1aq5w1grouaodn1pxwuvt9xy88z.htm) | To write serialized columner formats, you use [LIBNAME](https://documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/p1aq5w1grouaodn1pxwuvt9xy88z.htm), specify either an ORC or Parquet engine and specify ADLS storage parameters. In this case, </br> any file in the ADLS folder path specified will become a SAS dataset in the created *libref* consistant with the SAS terminology above. </br> However, you are additionally able to set a [`DIRECTORIES_AS_DATA`](https://documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/n0wvzanujcpw7wn150kdmmxcozpp.htm) argument which will create a SAS data set for every subfolder. While this setting </br> makes the *libref* inconsistant with the above SAS terminology, it is helpful when working with Hadoop and Databricks environments where the default </br> behavior is to name the table based upon the folder name, not individual file names. |
# MAGIC
# MAGIC The rest of this notebook will go through the initial configuration of ADLS and provide and write a free text example to confirm permissions.
# MAGIC
# MAGIC There are two SAS Community Entires that can be used for configurations:
# MAGIC  * [](https://communities.sas.com/t5/SAS-Communities-Library/SAS-Viya-Azure-AD-Single-Sign-On-to-Other-Azure-Services/ta-p/831206)
# MAGIC  * [SAS Viya 3.5 : CAS accessing Azure Data Lake files.](https://communities.sas.com/t5/SAS-Communities-Library/SAS-Viya-3-5-CAS-accessing-Azure-Data-Lake-files/ta-p/635147)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ADLS Initial Configuration
# MAGIC
# MAGIC Whenever you are interacting with ADLS from SAS you are leveraging the [Azure Access Method](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) which requires some configuration before first use. You must:
# MAGIC  - Create a [storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview) and container that you want to use with SAS
# MAGIC  - Register your SAS application, details can be found in [Access Azure Data Lake Files](https://communities.sas.com/t5/SAS-Communities-Library/SAS-Viya-3-5-CAS-accessing-Azure-Data-Lake-files/ta-p/635147)
# MAGIC  - Back in the storage account, you will need to grant the registered application the blob contributor role, again details can be found in [Access Azure Data Lake Files](https://communities.sas.com/t5/SAS-Communities-Library/SAS-Viya-3-5-CAS-accessing-Azure-Data-Lake-files/ta-p/635147)
# MAGIC  - After the initial attempt to use the application id, you will be required to authenticate using a Device Code and an Azure device registration page. Details of this process are found in [Obtaining an Authorization Device Code to Access ADLS Data](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm#p12td1al135jsun1fej3e09p4kpb).
# MAGIC  - After that, you will be able to continue to use the registered device code. Thoase credentials will be saved as a json file and the location of the file is determined by the [AZUREAUTHCACHELOC System Option](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lesysoptsref/p1xiwtwdy48nqxn18vh72yywv8nu.htm).
# MAGIC
# MAGIC Through this process we are going to need to reference the following variables, each of which we will set in our runtime configuration so that we can reference them when needed:
# MAGIC | Varaiable | Description |
# MAGIC | --------- | ----------- |
# MAGIC | `AZ_TENANT_ID`         | Available from the Azure Registered Application page for your organization |
# MAGIC | `ADLS_APPLICATION_ID`  | Available from the Azure Registered Application page for your organization |
# MAGIC | `ADLS_ACCOUNT_NAME`    | Azure Data Lake Storage account name |
# MAGIC | `ADLS_FILESYSTEM`      | ADLS container file system name |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## FILENAME ADLS Configuration
# MAGIC
# MAGIC We will use the following syntax to write files to ADLS using FILENAME:
# MAGIC ```SAS
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC
# MAGIC filename &FILEREF_NAME adls "&FILE_PATH"
# MAGIC    applicationid="&ADLS_APPLICATION_ID"
# MAGIC    accountname="&ADLS_ACCOUNT_NAME"
# MAGIC    filesystem="&ADLS_FILESYSTEM";
# MAGIC ```
# MAGIC
# MAGIC In this case, we have two parameters specific to the fileref we are creating:
# MAGIC | Varaiable | Description |
# MAGIC | --------- | ----------- |
# MAGIC | `FILEREF_NAME`         | The name of the file reference |
# MAGIC | `FILE_PATH`            | The path with file name used |
# MAGIC
# MAGIC An example SAS file for this code can also be found in <a href="$../sas/05-External_Location_ADLS_filename.sas" target="_blank">05-External_Location_ADLS_filename.sas</a>.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC As example, we will write a CSV file by line to ADLS, then show we can read that file in Databricks.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/external_location_data.png" alt="external_location_data" width="600px">
# MAGIC

# COMMAND ----------

path = 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/filename/example.csv'
dbutils.fs.rm(path)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC
# MAGIC filename example adls "external/demo/filename/example.csv"
# MAGIC    applicationid="&ADLS_APPLICATION_ID"
# MAGIC    accountname="&ADLS_ACCOUNT_NAME"
# MAGIC    filesystem="&ADLS_FILESYSTEM";
# MAGIC
# MAGIC data _null_;     
# MAGIC    file example;
# MAGIC    put 'a,b,c';
# MAGIC    put '10,20,30';
# MAGIC    put '11,21,31';
# MAGIC    put '12,22,32';
# MAGIC run;

# COMMAND ----------

import pyspark.pandas as ps
display(ps.read_csv('abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/filename/example.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # COMMENT ON PARALLEL READ/WRITE to ADLS
# MAGIC
# MAGIC TODO: Write out constraints on parallel reads / parallel writes. Currently, there is no parallel read or write functionality for ORC or Parquet. However, there is documented use of it for NFS, but not clear if it extends to ADLS. This will have to be proven out, before presenting TEXT as a viable storage format when using CAS.
# MAGIC
# MAGIC ![Path Caslib Data Access](https://go.documentation.sas.com/api/docsets/casref/v_002/content/images/path.png?locale=en) </br>
# MAGIC ![DNFS Caslib Data Access](https://go.documentation.sas.com/api/docsets/casref/v_002/content/images/dnfs.png?locale=en)
# MAGIC
# MAGIC https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/casref/p0hpbxsk1a8lpqn157winmr3cfop.htm
# MAGIC
# MAGIC | Method | Advantages | Disadvantages |
# MAGIC | ------ | ---------- | ------------- |
# MAGIC | Assign a Path or DNFS caslib to access the directory. | * The server accesses the data and avoids delays that are added by transferring the data through additional clients. </br> * For a DNFS caslib, a distributed server can load the data **in parallel** from the directory. | * If you need to load a CSV file that is not rigidly structured, CAS has limited ability to process record-by-record differences. |
# MAGIC | Assign a SAS libref to access the directory and then load from SAS to CAS. | \* To load a loosely structured CSV file, PROC IMPORT or a DATA step can perform sophisticated data type conversion and data manipulation. | * Unless data manipulation is required, SAS transfers the data serially to CAS and can be slow due to network transfer speeds |
