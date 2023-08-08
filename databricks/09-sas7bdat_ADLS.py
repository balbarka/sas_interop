# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SAS7BDAT Files
# MAGIC
# MAGIC **SAS7BDAT** files are binary database files that contain a header and data pages. The header lists the database's ID, name, contents, and other metadata. The data pages contain subheaders that include additional metadata, as well as the packed binary data that represents the database's dataset. This notebook will show methods to save and read SAS7BDAT files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC To streamline the movement of SAS files from SAS Cloud environment directly to ADLS, we'll create a mount and use SASPy to download a sas7bdat file directly to our ADLS sas7bdat directory.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/sas7bdat_saspy.png" alt="sas7bdat_saspy" width="600px">

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": spark.conf.get("spark.adls_client_id"),
           "fs.azure.account.oauth2.client.secret": spark.conf.get("spark.adls_client_secret"),
           "fs.azure.account.oauth2.client.endpoint": spark.conf.get("spark.adls_client_endpoint")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = "abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/sas7bdat",
                 mount_point = "/mnt/sas7bdat",
                 extra_configs = configs)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Use file details to get the path of our sas7bdat file */
# MAGIC PROC CONTENTS data = sashelp.cars;
# MAGIC RUN;

# COMMAND ----------

sas_magic.download('/dbfs/mnt/sas7bdat/',
                   '/opt/sas/viya/home/SASFoundation/sashelp/cars.sas7bdat',
                   True)

# COMMAND ----------

# MAGIC %md
# MAGIC While mounted, you can use the native [read_sas](https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html)

# COMMAND ----------

# Since we really don't need the mount beyond this demo, we can now unmount
dbutils.fs.unmount("/mnt/sas7bdat")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read SAS7BDAT files in Spark
# MAGIC
# MAGIC Now that we have an accessible copy of a SAS7BDAT file and after we have installed [](https://spark-packages.org/package/saurfang/spark-sas7bdat) to our cluster. We are able to use any of the following APIs directly in Databricks:
# MAGIC  * [pyspark API](https://github.com/saurfang/spark-sas7bdat#python-api)
# MAGIC  * [Spark SQL API](https://github.com/saurfang/spark-sas7bdat#sql-api)
# MAGIC  * [Spark Scala API](https://github.com/saurfang/spark-sas7bdat#scala-api)

# COMMAND ----------

cars = spark.read.format("com.github.saurfang.sas.spark") \
            .load("abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/sas7bdat/cars.sas7bdat",
                  forceLowercaseNames=True,
                  inferLong=True)
display(cars)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS cars;
# MAGIC CREATE TEMPORARY VIEW cars
# MAGIC USING com.github.saurfang.sas.spark
# MAGIC OPTIONS (path="abfss://sas-interop@hlsfieldexternal.dfs.core.windows.net/external/demo/sas7bdat/cars.sas7bdat");
# MAGIC SELECT * FROM cars LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We still have an inefficiency in the above process. We had to copy our data from SAS Cloud to ADLS. However, if our SAS Viya cloud deployment is already in ADLS, we can create an external location directly on ADLS storage and access the raw sas7bdat files from there without needing to create a copy.
# MAGIC
# MAGIC **TODO**: document and demo creating an external location with SAS Viya V9 Engine directories.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/sas7bdat_adls.png" alt="sas7bdat_adls" width="600px">
# MAGIC
