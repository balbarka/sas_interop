# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC There is a SAS solution for connecting to SAS as the Datasource using ODBC / JDBC. This is not common but does exist.
# MAGIC
# MAGIC While there hasn't been a release for a while, it is the SAS supported method for reading SAS BASE libraries directly. There are two Driver types:
# MAGIC  * [SAS Drivers for JDBC](https://documentation.sas.com/doc/en/jdbcref/9.4/n1rnpdq7idcrnyn17s5hv9g6x3rd.htm)
# MAGIC  * [SAS Drivers for ODBC](https://documentation.sas.com/doc/en/odbcdref/9.4/titlepage.htm)
# MAGIC
# MAGIC This would be used with an instance of [SAS/SHARE](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/shrref/titlepage.htm) which was only supported through **SAS 9.4 / Viya 3.5**.
