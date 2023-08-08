# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC The databricks SQL connector for python is the connection method we want to use for high performance transfer using pyarrow. The advantage here is that there are no additional JDBC / ODBC drivers or installs. This makes management of the installation much easier.
# MAGIC
# MAGIC Similar to Databricks connect, this can be used directly within a SAS session using PROC python or it can be used interactively within a jupyter notebook for interactive programming.
# MAGIC
# MAGIC ```python
# MAGIC from databricks import sql
# MAGIC import os
# MAGIC
# MAGIC connection = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
# MAGIC                          http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
# MAGIC                          access_token    = os.getenv("DATABRICKS_TOKEN"))
# MAGIC
# MAGIC cursor = connection.cursor()
# MAGIC
# MAGIC cursor.execute("SELECT * from range(10)")
# MAGIC print(cursor.fetchall())
# MAGIC
# MAGIC cursor.close()
# MAGIC connection.close()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The demonstration here, should be how to read the data to pandas, then to Databricks, then save to delta. We also want to highlight the pandas [read_sas](https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html)

# COMMAND ----------

from databricks import sql
import os

with sql.connect(server_hostname = spark.conf.get("spark.dbr_hostname"),
                 http_path       = spark.conf.get("spark.dbr_http_path"),
                 access_token    = spark.conf.get("spark.dbr_token")) as connection:


  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM sas_interop.demo.jdbc_cars LIMIT 5")
    result = cursor.fetchall()

    for row in result:
      print(row)


# COMMAND ----------

sql.__file__

# COMMAND ----------

https://learn.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector
