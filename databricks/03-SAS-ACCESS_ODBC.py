# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC TODO: Work not started requires additional configuraiton.
# MAGIC
# MAGIC From the SAS [https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1ht6mv0wybbzkn1x4np6nnlwzu6.htm](Support for ODBC on the SAS Viya Platform) documentation, there will be two additional configurations:
# MAGIC  - Install ODBC Driver Manager
# MAGIC  - Install ODBC Driver
# MAGIC
# MAGIC [Support for ODBC on the SAS Viya Platform](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1ht6mv0wybbzkn1x4np6nnlwzu6.htm)
# MAGIC
# MAGIC Environment Variables for the driver manager: `LD_LIBRARY_PATH`, `LIBPATH`, `SHLIB_PATH`
# MAGIC
# MAGIC Environment variables for **ODBC.INI** and **ODBCINST.INI** files: `ODBCINI`,`ODBCSYSINI`

# COMMAND ----------


