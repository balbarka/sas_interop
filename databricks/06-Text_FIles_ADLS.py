# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Export Text Files to ADLS
# MAGIC
# MAGIC We'll show two appraoched on how to write text files to ADLS:
# MAGIC  * By line input to a text file using a [data step](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsref/p1bp8z934fjg2pn1rjlh9vrqq0iv.htm#n00ebkyjnimfijn15wzyfhzmlsy8) step with [file](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsref/n15o12lpyoe4gfn1y1vcp6xs6966.htm#n1pyebpstm8ukbn1o7wqwrp9n7k9) statement. 
# MAGIC  * Use [PROC EXPORT](https://documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n0ku4pxzx3d2len10ozjgyjbrpl9.htm) to write a dataset leveraging a DBMS=DLM. 
# MAGIC  
# MAGIC In both cases we are able to reuse the same FILENAME statement from <a href="$./05-External_Location_ADLS" target="_blank">05-External_Location_ADLS</a>. 
# MAGIC
# MAGIC An example SAS file for this code can also be found in <a href="$../sas/06-Text_Files_ADLS.sas" target="_blank">06-Text_Files_ADLS.sas</a>.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/external_location_export.png" alt="external_location_export" width="600px">

# COMMAND ----------

path = 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/export_cars_csv/export_cars.csv'
dbutils.fs.rm(path)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC options azuretenantid = "&AZ_TENANT_ID";
# MAGIC
# MAGIC data cars;
# MAGIC set sashelp.cars;
# MAGIC format MSRP Invoice _NUMERIC_;
# MAGIC run;
# MAGIC
# MAGIC filename cars_csv adls
# MAGIC    "external/demo/export_cars_csv/export_cars.csv"
# MAGIC    applicationid="&ADLS_APPLICATION_ID"
# MAGIC    accountname="&ADLS_ACCOUNT_NAME"
# MAGIC    filesystem="&ADLS_FILESYSTEM"
# MAGIC    encoding="utf-8";
# MAGIC
# MAGIC proc export data=cars
# MAGIC    outfile=cars_csv
# MAGIC    dbms=dlm replace;
# MAGIC    delimiter=',';
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.export_cars_csv;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.export_cars_csv (
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
# MAGIC USING CSV
# MAGIC OPTIONS (delimiter ",", header "true")
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/export_cars_csv';
# MAGIC SELECT * FROM sas_interop.demo.export_cars_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # TXT FILE TABLE
# MAGIC
# MAGIC Not that all the options need to be explored, but file can also be written using different delimiter arguments and can also be compressed with GZIP:

# COMMAND ----------

path = 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/export_cars_txt/export_cars.txt'
dbutils.fs.rm(path)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC filename cars_txt adls
# MAGIC    "external/demo/export_cars_txt/export_cars.txt"
# MAGIC    applicationid="&ADLS_APPLICATION_ID"
# MAGIC    accountname="&ADLS_ACCOUNT_NAME"
# MAGIC    filesystem="&ADLS_FILESYSTEM";
# MAGIC
# MAGIC
# MAGIC proc export data=cars
# MAGIC    outfile=cars_txt
# MAGIC    dbms=TAB replace;
# MAGIC run;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sas_interop.demo.export_cars_txt;
# MAGIC CREATE EXTERNAL TABLE sas_interop.demo.export_cars_txt (
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
# MAGIC USING CSV
# MAGIC OPTIONS (delimiter "\t", header "true")
# MAGIC LOCATION 'abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/export_cars_txt';
# MAGIC SELECT * FROM sas_interop.demo.export_cars_txt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT type, count(*) cnt FROM sas_interop.demo.export_cars_txt GROUP BY 1;
