# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS/ACCESS
# MAGIC
# MAGIC **SAS/ACCESS** is a suite of products within the SAS software ecosystem that provides connectivity between SAS and external data sources, such as relational databases or big data platforms. The suite satisfies the following requirements:
# MAGIC
# MAGIC  - **Data Access**: SAS/ACCESS allows you to access data stored in external sources directly from within SAS, without the need to extract and transform the data into SAS datasets. This enables you to work with large and diverse datasets without data duplication.
# MAGIC  - **Push-down Optimization**: SAS/ACCESS optimizes data access and retrieval by generating native queries and utilizing database-specific optimizations, resulting in faster processing and improved performance.
# MAGIC  - **Data Security and Governance**: SAS/ACCESS provides a secure way to access and manage data from external sources while adhering to data governance and security policies. It supports features like data masking, encryption, and access controls.
# MAGIC
# MAGIC **NOTE**: Unlike previous sessions where the capability comes from the python language itself, data connectivity features are more integrated with platform architecture. This section will include features specific to Databricks when working in python.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Databricks addresses these requirements with the following features:
# MAGIC
# MAGIC  * **Data Access** - Data Connectivity is typically addressed in one of three ways:
# MAGIC      - [Python API via libraries](https://pypi.org/) - Any sufficiently popular database service will provide a python API. It is advised that you start with the vendor provided library as this is mostly likely to be kept up to date with any new features made avaialble. Typically these api operate locally creating pandas dataframes.
# MAGIC      - [Spark jdbc](https://learn.microsoft.com/en-us/azure/databricks/external-data/jdbc) - creates distributed concurrent connections by default. The configuration is just the existing jdbc connection configuration. This comes with intelligent pushdown to optimize what should be run in the remote data store and what is gathered and run in spark. This connection will create a spark dataframe.
# MAGIC      - [Lakehouse Federation](https://learn.microsoft.com/en-us/azure/databricks/query-federation/) - is functionality that comes with Unity Catalog. This allows you to store your external data service credentials in a UC object. From there you are able to create data catalogs that look like existing catalog except that the back end is an external data store.
# MAGIC
# MAGIC  * **Push-down optimization** - This functionality is in spark_jdbc and Lakehouse federation by default - there is **no further configuration necessary**.
# MAGIC
# MAGIC  * **Data Security & Goverance** - governance wil change depending on approach used, be aware of the following features for securing the credentials of the remote data source:
# MAGIC       - [Databricks Secrets](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets) - usually set via [cli](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets#create-a-secret-in-a-databricks-backed-scope) and referencable via [spark config](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets#create-a-secret-in-a-databricks-backed-scope). This appraoch will also redact any values that are secret from notebook output and logs while still making the value accessible to programs.
# MAGIC       - [Lakehouse Federation Securable Objects](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#overview-of-lakehouse-federation-setup) - There are two relavant objects in Lakehouse federation; 
# MAGIC            - A [connection](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-connection) is where you store the actual credentials and other remote data store configurations. Once provided, you can specify which users can manage the connection, but won't be able to retrieve secrets. Access to use the connection is governed by Unity Catalog security conventions.
# MAGIC            - A [foreign catalog](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-foreign-catalog) is then used to allow connection to the remote data store via the connection. Each foreign catalog is configured at the database level, thus it is common to have many foreign catalogs per single connection. **NOTE** you are able to again set pemissions via unity catalog conventions, but it is important to realize that these grants will all give permission to use the connection credentials. If you need to ensure that there is consistancy in permissions when accessing via Databricks foreign catalog or direct access to the remote data store, you will want to configure and use an identity and access managment service like [AAD](https://azure.microsoft.com/en-us/products/active-directory).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Secrets
# MAGIC
# MAGIC Many of the following configurations will require secrets to be configured. This section will go through an example of managing secrets using the databricks cli. To avoid the need to generate a long-lived [Databricks Personal Access Token](https://docs.databricks.com/en/dev-tools/auth.html#databricks-personal-access-token-authentication) we'll pull an existing token from the current notebook session and save as `TOKEN`.
# MAGIC
# MAGIC **NOTE**: It is a better practice to create a PAT. We are only using a session token for ease of demonstration.
# MAGIC
# MAGIC Then we will want to install the [Databricks CLI](Databricks CLI tutorial).
# MAGIC
# MAGIC **NOTE**: You would normally install this on a local instance or a jump server. It is only being installed within a cluster session for demonstration purposes.
# MAGIC
# MAGIC **NOTE**: In python, [subprocess](https://docs.python.org/3/library/subprocess.html) is used to run commandline processes.
# MAGIC
# MAGIC We will will write our credentials to the Databricks CLI configuration file.
# MAGIC
# MAGIC **NOTE**: On your local device it is advised to use the `databricks configure` and respond to prompts. We are only writing the configurations direct to file for ease of demonstration.
# MAGIC
# MAGIC With our cli installed, we can now use it to create a scope and set some sample secrets; `demo_user`, `demo_password.`

# COMMAND ----------

import subprocess

API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

process = subprocess.run('curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh',
                          capture_output=True, shell=True)

config_txt = f"""[DEFAULT]
host  = {API_URL}
token = {TOKEN}"""

with open('/root/.databrickscfg', 'w') as f:
    f.write(config_txt)

p_create = subprocess.run(['databricks', 'secrets', 'create-scope', 'demo'], capture_output=True)
p_set_usr = subprocess.run(['databricks', 'secrets', 'put-secret', 'demo',
                            'demo_user','--string-value=gump'], capture_output=True)
p_set_pwd = subprocess.run(['databricks', 'secrets', 'put-secret', 'demo',
                            'demo_password','--string-value=1forest1!'], capture_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Retreive Secrets
# MAGIC
# MAGIC Databricks allows the retrieval of secrets using `dbutils`. As a safety feature, the values are always redacted in notebook output and logs.
# MAGIC
# MAGIC You can use this this approach to configure connections while mitigating the risk of credentials exposure.

# COMMAND ----------

print('demo_user: ' + dbutils.secrets.get('demo', 'demo_user'))
print('part demo_user: ' + dbutils.secrets.get('demo', 'demo_user')[:-1])
print('demo_password: ' + dbutils.secrets.get('demo', 'demo_password'))
print('part demo_password: ' + dbutils.secrets.get('demo', 'demo_password')[:-1])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Example Python API to connect to Data Source (sql server)
# MAGIC
# MAGIC Now that we have credentials stored as secrets, we can use them in a python API, in this case sql server. In most cases you will need to first install the python library for the source data platform.
# MAGIC
# MAGIC Once we have the library installed, we can run a query to verify the connection, `conn`, works. We can create a connection instance and use within pandas to run a query.
# MAGIC
# MAGIC This `conn` can be used for a couple pandas methods:
# MAGIC
# MAGIC  * [read_sql_table](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html#pandas.read_sql_table) - Reads entire table in to a pandas dataframe.
# MAGIC  * [read_sql_query](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html#pandas.read_sql_query) - Executes a query and reads into pandas dataframe. 
# MAGIC  * [read_sql](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html) - A Convenience wrapper that will execute `read_sql_table` or `read_sql_query` depending on arguments provided.
# MAGIC
# MAGIC
# MAGIC **NOTE**: We will actually be using different secrets than those in the demo, but the syntax will be exactly the same.

# COMMAND ----------

# MAGIC %pip install pymssql

# COMMAND ----------

import pandas as pd
import pymssql

conn = pymssql.connect(server='hls-field-sql.database.windows.net', 
                       user=dbutils.secrets.get('jdbc_credentials', 'user'), 
                       password=dbutils.secrets.get('jdbc_credentials', 'password'), 
                       database="hls-field-sql")

sql = "SELECT * FROM products"

rslt_pdf = pd.read_sql(sql, conn)
display(rslt_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Example Spark JDBC (sql server)
# MAGIC
# MAGIC Similar to how we configure a method to run a query with results returned to a pandas dataframe. Spark has a method to run a query and return results to a spark dataframe.
# MAGIC
# MAGIC It is important to understand the distinction between these two query examples. When you are using spark you are able to take advantage of distributed computing and processing of larger datasets. This is comparable to using CAS in SAS. However, you will notice that we didn't need to instantiate or declare a CAS environment because with Databricks spark is a first class citizen and is instantiated automatically at cluster startup. In fact, the cluster configurations for worker selection are directly used in you spark cluster. Thus, if you ever need a larger spark instance you will make that update in your cluster configs.

# COMMAND ----------

jdbc_kwargs = {"url": f'jdbc:sqlserver://hls-field-sql.database.windows.net:1433;database=hls-field-sql;' +
               'encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;',
               "user": dbutils.secrets.get('jdbc_credentials', 'user'),
               "password": dbutils.secrets.get('jdbc_credentials', 'password')}

dat = spark.read.format("jdbc").options(**jdbc_kwargs) \
                               .option("query", sql).load()

display(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Lakehouse Federation Example (sql server)
# MAGIC
# MAGIC Lakehouse Federation allows the creation of a remote datasource as a Unity Catalog catalog. It operates using the following entities:
# MAGIC
# MAGIC  * A [connection](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-connection) is where you store the actual credentials and other remote data store configurations. In our case, we'll create using only SQL, but you can check out the [documentation](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-connection) if you want to create a connection using the UI.
# MAGIC  * A [foreign catalog](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-foreign-catalog) is then used to allow connection to the remote data store via the connection. Each foreign catalog is configured at the database level, thus it is common to have many foreign catalogs per single connection. **NOTE** you are able to again set pemissions via unity catalog conventions, but it is important to realize that these grants will all give permission to use the connection credentials. If you need to ensure that there is consistancy in permissions when accessing via Databricks foreign catalog or direct access to the remote data store, you will want to configure and use an identity and access managment service like [AAD](https://azure.microsoft.com/en-us/products/active-directory).
# MAGIC
# MAGIC  **NOTE**: To be able to our varaiables accessible in the sql context, we've set in python and call them in sql. Be aware that you can also set these variables as part of your cluster configuration at startup without exposing any secrets. Check out [Reference a secret with a Spark configuration property](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets#--reference-a-secret-with-a-spark-configuration-property).
# MAGIC
# MAGIC **NOTE**: You are also able to access secrets directly with a secret method in sql. To show both approaches, we'll create our connection where the user secret uses a spark conf and the password is set using the [secret](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/secret) function.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [CREATE A CONNECTION](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-connection)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CONNECTION demo_sql_server TYPE sqlserver
# MAGIC OPTIONS (
# MAGIC   host "hls-field-sql.database.windows.net",
# MAGIC   port "1433",
# MAGIC   user secret("jdbc_credentials","user"),
# MAGIC   password secret("jdbc_credentials","password")
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [CREATE A FOREIGN CATALOG](https://learn.microsoft.com/en-us/azure/databricks/query-federation/#--create-a-foreign-catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS demo_sql_server USING CONNECTION demo_sql_server
# MAGIC OPTIONS (database 'hls-field-sql');

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now we can inspect our connection and foriegn catalog. Finally, we can query our remote data source as if it were a Managed table in unity catalog:

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CONNECTIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'demo_sql_server'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_sql_server.dbo.products;
