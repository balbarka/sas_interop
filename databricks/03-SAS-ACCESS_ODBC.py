# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC From the SAS [https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1ht6mv0wybbzkn1x4np6nnlwzu6.htm](Support for ODBC on the SAS Viya Platform) documentation, ther are some pre-configurations:
# MAGIC  - Install ODBC Driver Manager
# MAGIC  - Install ODBC Driver
# MAGIC
# MAGIC
# MAGIC ![odbc_arch](https://documentation.sas.com/api/docsets/acreldb/v_003/content/images/odbc.svg?locale=en)
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/odbc_connection.png" alt="odbc_connection" width="500px">
# MAGIC
# MAGIC Environment Variables for the driver manager: `LD_LIBRARY_PATH`, `LIBPATH`, `SHLIB_PATH`
# MAGIC
# MAGIC Environment variables for **ODBC.INI** and **ODBCINST.INI** files: `ODBCINI`,`ODBCSYSINI`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Have had issues getting the Databricks ODBC driver to work on SAS Viya 4.0. Haven't figured out if this is a problem with how I installed the driver or set env variables or if there is something I should submit a ticket on. Here is what happens:
# MAGIC I can connect without issue the the Databricks cluster using JDBC:
# MAGIC
# MAGIC ![sas_jdbc](https://github.com/balbarka/sas_interop/raw/main/ref/img/JDBC_SAS.png)
# MAGIC
# MAGIC I can see the query that comes through on the JDBC Session side, makes sense:
# MAGIC
# MAGIC ![sas_jdbc](https://github.com/balbarka/sas_interop/raw/main/ref/img/JDBC_Session_Query.png)
# MAGIC
# MAGIC Then I try the same for ODBC, but get an odd configurations:
# MAGIC
# MAGIC ![sas_jdbc](https://github.com/balbarka/sas_interop/raw/main/ref/img/ODBC_SAS.png)
# MAGIC
# MAGIC When I check the ODBC session query, I see a non-sql command:
# MAGIC
# MAGIC ![sas_jdbc](https://github.com/balbarka/sas_interop/raw/main/ref/img/ODBC_Session_Query.png)
# MAGIC
# MAGIC Since sas runs command on listing and metadata before executing procs, not getting a table listing back prevents any further use of the ODBC libname.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Would like to attempt with the CData driver, but don't currently have a copy.
# MAGIC
# MAGIC Reference links:
# MAGIC  * [ODBC Data Connector](https://documentation.sas.com/doc/en/pgmsascdc/v_035/casref/n14dfaaxna5uhmn16c65hdq4x3i4.htm) - SAS </br>
# MAGIC  * [CData ODBC Driver](https://cdn.cdata.com/help/LKH/odbc/RSBDatabricks_c_AzureAuthentication.htm) - CData
# MAGIC  * configure [ODBC ENV Variables](https://documentation.sas.com/doc/en/calcdc/3.5/dplyml0phy0lax/p03m8khzllmphsn17iubdbx6fjpq.htm#p1qkm4koo8isxyn1xgb55165fgq5)
# MAGIC  * [Support for ODBC on the SAS Viya Platform](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1ht6mv0wybbzkn1x4np6nnlwzu6.htm)
# MAGIC
# MAGIC
