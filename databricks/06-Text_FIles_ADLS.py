# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Export to ADLS
# MAGIC
# MAGIC In this pattern we will look at how to write text files so that they can be immediately read by 
# MAGIC
# MAGIC
# MAGIC # CSV FILE TABLE
# MAGIC
# MAGIC This process uses [FILENAME adls](https://documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) and [PROC EXPORT](https://documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n0ku4pxzx3d2len10ozjgyjbrpl9.htm) to write text files that can be immediately read in Databricks.

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
# MAGIC SELECT type, count(*) FROM sas_interop.demo.export_cars_txt GROUP BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Working with JSON
# MAGIC
# MAGIC We really want to avoid this, we'll create an example time permitting. This should only be explored if the existing SAS application is already writing json or jsonl files.
# MAGIC
# MAGIC Technically this uses jsonl which is a good format for nested data:
# MAGIC
# MAGIC https://blogs.sas.com/content/sasdummy/2018/11/14/jsonl-with-proc-json/
# MAGIC
# MAGIC This is a bad idea, don't do it

# COMMAND ----------


