# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Original notebooks content: [01-Reading-SAS-datasets](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/2192578327224554/command/2192578327224555)
# MAGIC Needs to be updated to sas_intop w/ reference on methods to export files.
# MAGIC
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Reading SAS data files into Databricks
# MAGIC
# MAGIC
# MAGIC **In this lesson you:**
# MAGIC 1. Learn about Databricks databases and tables and how they compare to SAS libraries and datasets
# MAGIC 1. Understand how to use named Databricks DataFrames and tables in a similar manner as SAS filename references 
# MAGIC 1. Read data in from various locations and write data out to DataFrames and tables 

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAS dataset libraries and Databricks databases and tables
# MAGIC
# MAGIC In SAS, a library points to a particular location on a drive where a collection of sas datasets are stored. This could be on a local network drive, a filesystem, or a remote database. Using the `libname` assigned to a library allows you to reference a dataset within a library.  
# MAGIC
# MAGIC In Databricks, this is equivalent to creating a [metastore database](https://docs.databricks.com/data/metastores/index.html) that points to some [unmanaged tables](https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables) and allows you to persist the tables outside of a session or an individual cluster.
# MAGIC
# MAGIC A Databricks database is a collection of tables. A [Databricks table](https://docs.databricks.com/data/tables.html#) is a collection of structured data. You can cache, filter, and perform any operations supported by Apache Spark DataFrames on Databricks tables. You can query tables with Spark APIs and Spark SQL.
# MAGIC
# MAGIC There are two types of tables: global and local. 
# MAGIC - A global table is available across all clusters. Databricks registers global tables either to the Databricks Hive metastore or to an external Hive metastore. 
# MAGIC - A local table is not accessible from other clusters and is not registered in the Hive metastore. This is also known as a temporary view.
# MAGIC
# MAGIC You can learn more about creating and managing databases, tables, and views in the Databricks Academy course "Quick Reference: Relational Entities on Databricks".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data storage and access
# MAGIC
# MAGIC Using SAS, you often use the `filename in` pattern to read data from external sources. In Databricks, you can use the [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) to access data in object storage.
# MAGIC
# MAGIC Databricks File System (DBFS) is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. DBFS is an abstraction on top of scalable object storage and offers the following benefits:
# MAGIC - Allows you to mount storage objects so that you can seamlessly access data without requiring credentials.
# MAGIC - Allows you to interact with object storage using directory and file semantics instead of storage URLs.
# MAGIC - Persists files to object storage, so you won’t lose data after you terminate a cluster.
# MAGIC
# MAGIC [Mounting object storage](https://docs.databricks.com/data/databricks-file-system.html#mount-object-storage-to-dbfs) to DBFS allows you to access objects in object storage (i.e. AWS S3 buckets, Azure Blob storage containers, etc.) as if they were on the local file system. In this course, we have already mounted a data file to DBFS and will access the file from there.
# MAGIC
# MAGIC An example of the equivalent in SAS:
# MAGIC
# MAGIC `filename in s3 '/directory1/i.csv' ;
# MAGIC `

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run the cells below to get started.

# COMMAND ----------

# MAGIC %run ./Includes/classroom-setup

# COMMAND ----------

# create a pointer to a sas7bdat file in the source directory that has been set up for you in cloud object storage:
sasfile = sourcedir + 'allergies.sas7bdat'

# create a filepath location for the dataset in the Databricks File System (DBFS)
filepath = working_dir + "/rawdata/allergies.sas7bdat"

# copy the file from the source directory to the filepath location
dbutils.fs.cp(sasfile, filepath)

# list the files in the dbfs location
dbutils.fs.ls(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read a dataset from a specified location
# MAGIC
# MAGIC In SAS, you read a dataset in using the `set` method:
# MAGIC
# MAGIC   `Data a;  
# MAGIC       Set b;  
# MAGIC       Run;`
# MAGIC
# MAGIC There are several methods we can use to read a SAS dataset in and write to a Databricks format:
# MAGIC  1. Write to a Spark DataFrame using Python
# MAGIC  1. Write to a SQL table or a temporary view using Python or SQL
# MAGIC  1. Write to a Delta table using Python
# MAGIC  
# MAGIC We will load a sas7bdat file using the [saurfang](https://github.com/saurfang/spark-sas7bdat) library format option. In order to read in sas7bdat files, we need to use the saurfang library that we installed in our cluster. 
# MAGIC
# MAGIC We will demonstrate two methods for loading the dataset:
# MAGIC 1. Loading the data into a table using SQL
# MAGIC 1. Loading the data and writing it to a Spark DataFrame using Python.

# COMMAND ----------

# get the path for the dataset in dbfs
print(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC **In the cell below, copy the filepath output from the previous cell and paste it in where you see FILL IN.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW allergies
# MAGIC USING com.github.saurfang.sas.spark
# MAGIC OPTIONS (PATH 'dbfs:/user/mina_rao_databricks_com/dbacademy/sasproc/rawdata/allergies.sas7bdat')

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the table was created correctly by running a SQL query:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM allergies

# COMMAND ----------

# MAGIC %md
# MAGIC You can now issue any SQL queries you want on the `allergies` table.
# MAGIC
# MAGIC Below, we will show how you can write the data to a DataFrame instead, and various methods you can use on a DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Write to a Spark DataFrame using Python
# MAGIC
# MAGIC The code block below returns a DataFrame `df`. A DataFrame is a two-dimensional labeled data structure with columns of potentially different types. You can think of a DataFrame like a spreadsheet, a SQL table, or a dictionary of series objects. 
# MAGIC
# MAGIC The Spark DataFrameReader is called via `spark.read` and is similar to the `proc import` command in SAS. The DataFrameReader can take in many file formats, such as json, parquet, csv, and many more, and you can specify many options. You can learn more about this in the [official documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output). For more information and examples, see the [Quickstart](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html) on the Apache Spark documentation website.
# MAGIC
# MAGIC NOTE: Here, we specify the input schema for the data by listing the field names and their data types. If we do not specify a schema, the DataFrameReader will infer one by sampling some of the input file.

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark.read
           .format("com.github.saurfang.sas.spark")
           .schema("start DATE, stop STRING, patient STRING, encounter STRING, code LONG, description STRING")
           .load(filepath, forceLowercaseNames=True)
     )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can also use the `display` command to see the data in tabular format. 
# MAGIC
# MAGIC NOTE: You can display plots of the DataFrame and specify options, using the buttons below the output. 
# MAGIC
# MAGIC **Try it: display the output as a bar chart and set the Plot Options to use `description` as the Key, `patient` as the Value, and `COUNT` as the Aggregation.**

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can work with this dataset just like we would with any Spark DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write to a table or a temporary view using PySpark
# MAGIC
# MAGIC Before you can issue SQL queries on a DataFrame, you must save it as a table or temporary view. Temporary views are removed once your session has ended, whereas tables are persisted beyond a given session.
# MAGIC
# MAGIC A [Databricks table](https://docs.databricks.com/data/tables.html) is a collection of structured data. You can cache, filter, and perform any operations supported by Apache Spark DataFrames on Databricks tables. You can query tables with Spark APIs and Spark SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary table/view from a DataFrame using Python and then query the table using SQL:

# COMMAND ----------

df.createOrReplaceTempView("allergies")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM allergies

# COMMAND ----------

# MAGIC %md
# MAGIC You can delete the table, without deleting the DataFrame or original data, using SQL:

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE allergies

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write to a Delta table using Python
# MAGIC
# MAGIC We can also write the DataFrame to a Delta table.
# MAGIC
# MAGIC For more on Delta tables, see the [Quickstart](https://docs.databricks.com/delta/quick-start.html) on the Databricks documentation site.

# COMMAND ----------

(df.write
   .format("delta")
   .mode("append")
   .save(working_dir + "/allergies_table")
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now list the contents of our userhome directory and see the `allergies_table` Delta table, along with the `rawdata` folder we created at the beginning of this notebook:

# COMMAND ----------

dbutils.fs.ls(working_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC In the next notebook, we will explore how to perform some common SAS DATA Steps in Databricks.
