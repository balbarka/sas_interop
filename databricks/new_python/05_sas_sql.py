# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS SQL 
# MAGIC
# MAGIC SAS SQL opens up all of the functionality of ANSI SQL and is executed primarily through two SAS functions:
# MAGIC  * [PROC SQL](https://go.documentation.sas.com/doc/en/pgmsascdc/v_037/sqlproc/n0w2pkrm208upln11i9r4ogwyvow.htm)
# MAGIC  * [PROC FEDSQL](https://go.documentation.sas.com/doc/en/pgmsascdc/v_037/proc/n06w5kqwkurgk2n1irjo6h94fkcq.htm) 
# MAGIC
# MAGIC In SAS, both PROC FEDSQL and PROC SQL are used for querying and manipulating data using SQL syntax. However, PROC FEDSQL is specifically designed for use in a distributed computing environment. In Databricks, spark is the distributed framework thus spark SQL is distributed by default. The most common PROC is PROC SQL, thus going forward we'll refer to SAS SQL as either, but comments around syntax be for PROC SQL. 
# MAGIC
# MAGIC SAS is a procedural language and its use of PROC SQL is consistent with that experience. SAS uses PROC SQL similar to data step and describes it as much in the [PROC SQL documentation](https://go.documentation.sas.com/doc/en/pgmsascdc/v_037/sqlproc/n1oihmdy7om5rmn1aorxui3kxizl.htm):
# MAGIC
# MAGIC PROC SQL has been in SAS since the early 1980s and is a mature SQL syntax. However it supports more than ANSI SQL and so does Databricks SQL. Since base or ANSI SQL is the same between both, we'll focus on the features in SAS SQL that may be written differently in Databricks:
# MAGIC
# MAGIC  * **SQL Functions** - Databricks comes with built in functions just like SAS. If you are using a SAS built-in function and get a syntax error, it is likely that the function is named different or has different arguments. When this occures you can check out the list of existing [Databricks Built-in SQL functions](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin). In cases that there isn't comparable built-in funcitons in Databricks, you can use [Databricks User Defined Functions](https://docs.databricks.com/en/udf/index.html) (UDFs). These are covered in more detail below.
# MAGIC
# MAGIC  * **PROC SQL format syntax** - for user familiarity SAS offers the ability to format using `format=`. This is functionally equicalent to using the [cast](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/cast) built in function.
# MAGIC
# MAGIC  * **PROC SQL Merge** - for user familiarity SAS offers `merge` syntax for joining tables. These joins can also be done using ANSI SQL and is supported in Databricks. Be aware that Databricks also treats `MERGE` as a reserved keyword as used in ANSI SQL. When [MERGE INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-merge-into) is used, it is not to join two tables, but to add, update, or delete records from a source DataFrame into a DeltaLake table.
# MAGIC
# MAGIC  * **Procedures Integration** - is a SAS concept where users are able to seemlessly take the output of one procedure and use as input to a following procedure of same for different type. This allows users to construct data steps, proc sql, and modeling proc in series to create a works flow in a single SAS script. Databricks SQL operations are based in spark. The python API for spark is pySpark. PySpark, Spark SQL, and Spark ML can all applied in any order similar the procedures. An example of switching between Spark SQL and pyspark api will be provided below.
# MAGIC
# MAGIC  * **Libref Metadata** - as a convenience feature in SAS, you are able to navigate your library refrences in the SAS side panel. This functionality is helpful to inspect the metadata of tables. The euivalent functionality in Databricks is the **data explorer** which will list all tables users have access to in unity catalog. This includes catalog.database.table hierachy and search for each of access to metadata and permissions. There are system maintained tables in Databricks comparable to SAS dictionary tables, those are found in the **system** catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Like PROC SQL, there is an ease of development by being able to write SQL without having to manage escapes. This is done in Databricks by using ipython magic syntax to change the cell to SQL. Below are some basic view creation and an example join.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create some temp views so we can demonstrate sql syntax
# MAGIC CREATE TEMP VIEW customers (cust_id, first, last, location) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (1, 'John', 'Doe', 'Chicago'),
# MAGIC   (2, 'Jane', 'Doe', 'New York'),
# MAGIC   (3, 'Bob', 'Smith', 'San Francisco'));
# MAGIC CREATE TEMP VIEW products (cust_id, product) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (1, 'lamp'),
# MAGIC   (2, 'stapler'),
# MAGIC   (3, 'pencil'));

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.*,
# MAGIC     p.product
# MAGIC FROM
# MAGIC     customers c
# MAGIC LEFT JOIN
# MAGIC     products p
# MAGIC ON
# MAGIC     c.cust_id = p.cust_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Pyspark API
# MAGIC
# MAGIC the pySpark API has an entry class, `spark`, that you will use to access the same context that has the views above defined. Here is an example of that join using this entry class and a python string.

# COMMAND ----------

join_sql = """
SELECT
    c.*,
    p.product
FROM
    customers c
LEFT JOIN
    products p
ON
    c.cust_id = p.cust_id
"""

display(spark.sql(join_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The pySpark API can also have methods on it's classes. One of the most common classes used is the DataFrame class,  `pyspark.sql.dataframe.DataFrame`. When working with the DataFrame class, it is most common to write join using the class methods. Here is an example of the syntax used join our two temp tables using the pySpark API.
# MAGIC
# MAGIC **NOTE**: Thee table method in this case is able to return either tables or views.

# COMMAND ----------

cust = spark.table('customers')
prod = spark.table('products')
cust.__class__

# COMMAND ----------

rslt = cust.join(prod, on='cust_id', how='left')
display(rslt)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Usually you will want to write and read from a persisted source. In Databricks there is a hierachy of `catagory`.`database`.`schema`. The default catagory is main, which we will use here, but explicitly provide here for clarity.
# MAGIC
# MAGIC **NOTE**: Just as in SAS you can persist files in format and location of users choice. That detail is beyond this introduction, but for clarity, the location is Managed by Databricks Unity Catalog and the format defaults to delta. We'll look at table details later to show this.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.products AS
# MAGIC SELECT * FROM products;
# MAGIC
# MAGIC SELECT * FROM main.default.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We don't like the cust 2 record, let's delete it
# MAGIC DELETE FROM main.default.products 
# MAGIC WHERE cust_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now let's add records using the ANSI sql statement
# MAGIC INSERT INTO main.default.products
# MAGIC VALUES
# MAGIC   (2, 'carr'),
# MAGIC   (4, 'box');
# MAGIC SELECT * FROM main.default.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uh oh, we misspelled cars, here is the ANSI sql to update
# MAGIC UPDATE main.default.products
# MAGIC    SET product='car'
# MAGIC    WHERE product='carr';
# MAGIC
# MAGIC SELECT * FROM main.default.products;

# COMMAND ----------

# MAGIC %md
# MAGIC There are situations where it is important to conditionally update, delete, and insert in a single atomic transaction. The following example will update or insert records:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS new_prod_records;
# MAGIC CREATE TEMP VIEW new_prod_records (cust_id, product) AS
# MAGIC SELECT * FROM (
# MAGIC VALUES
# MAGIC   (5, 'can'),
# MAGIC   (6, 'paper'),
# MAGIC   (3, 'pen'));
# MAGIC
# MAGIC MERGE INTO main.default.products tgt
# MAGIC USING new_prod_records src
# MAGIC ON src.cust_id = tgt.cust_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.products;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Built into a Databricks table is the history of the table. To see more detail on what this history includes, check out [Delta Table HIstory](https://learn.microsoft.com/en-us/azure/databricks/delta/history#retrieve-delta-table-history).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY main.default.products;

# COMMAND ----------

# MAGIC %md
# MAGIC You also have the ability to time travel on this table by setting a version. For more options check out [Delta Time Travel](https://learn.microsoft.com/en-us/azure/databricks/delta/history#delta-time-travel-syntax).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.products VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC All done, let's delete our demo table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.products;
