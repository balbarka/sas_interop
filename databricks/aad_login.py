# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # TEST AAD authentication with Databricks ODBC Driver
# MAGIC
# MAGIC This notebook is to confirm that we can create a connection to Databricks using:
# MAGIC  - [pyodbc](https://pypi.org/project/pyodbc/) (4.0.39)
# MAGIC  - [Databricks ODBC Driver](https://www.databricks.com/spark/odbc-drivers-download) (2.6.26.1045 DEB 64)
# MAGIC  - [Databricks Runtime](https://adb-8590162618558854.14.azuredatabricks.net/?o=8590162618558854#setting/clusters) (12.2 LTS)
# MAGIC  - [Azure CLI](https://pypi.org/project/azure-cli/) (2.49.0)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Get AAD Token
# MAGIC
# MAGIC There are three steps required to attain an AAD token:
# MAGIC  - Install the azure-cli tool using pip
# MAGIC  - Create a device authentication request
# MAGIC  - Request a token after login
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC # Install the azure-cli tool
# MAGIC pip install azure-cli==2.49.0 &> /dev/null

# COMMAND ----------

# MAGIC %sh
# MAGIC # Create device request (will require opening a separate window for authenitcation)
# MAGIC az login --tenant 9f37a392-f0ae-4280-9796-f1864a10effc --use-device-code --output table

# COMMAND ----------

import subprocess
from subprocess import check_output
aad_token = check_output(["az", "account", "get-access-token",
                              "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                              "--query", "accessToken", "--output", "tsv"])[:-1].decode('utf-8')

print('aad_token: ...' + aad_token[-8:])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Install pyodbc and Databricks ODBC and Configure
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip /tmp/SimbaSparkODBC 
# MAGIC curl -o /tmp/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.26/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip &> /dev/null
# MAGIC unzip /tmp/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip -d /tmp/SimbaSparkODBC &> /dev/null
# MAGIC apt-get update -y &> /dev/null
# MAGIC apt-get install -y libsasl2-modules-gssapi-mit &> /dev/null
# MAGIC dpkg -i /tmp/SimbaSparkODBC/simbaspark_2.6.26.1045-2_amd64.deb &> /dev/null
# MAGIC rm -rf /tmp/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip /tmp/SimbaSparkODBC 

# COMMAND ----------

schema="demo"
catalog="sas_interop"

odbc_ini = \
f"""[ODBC Data Sources]
Databricks_PAT=Databricks ODBC Connector with PAT
Databricks_AAD=Databricks ODBC Connector with AAD

[Databricks_PAT]
Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so
Host={spark.conf.get("spark.dbr_hostname")}
Port=443
HTTPPath={spark.conf.get("spark.dbr_http_path")}
SSL=1
ThriftTransport=2
Schema={schema}
Catalog={catalog}
AuthMech=3
UID=token
PWD={spark.conf.get("spark.dbr_token")}

[Databricks_AAD]
Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so
Host={spark.conf.get("spark.dbr_hostname")}
Port=443
HTTPPath={spark.conf.get("spark.dbr_http_path")}
SSL=1
ThriftTransport=2
Schema={schema}
Catalog={catalog}
AuthMech=11
Auth_Flow=0
Auth_AccessToken="{aad_token}"
"""
with open('/etc/odbc.ini', 'w') as f:
    f.write(odbc_ini)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run tests with PAT/AAD, DSN/ConnString
# MAGIC
# MAGIC | Auth Method | DSN  | ConnectionString |
# MAGIC | ----------- | ---- | ---------------- |
# MAGIC | PAT         | PASS | PASS             |
# MAGIC | AAD         | FAIL | FAIL             |

# COMMAND ----------

import pyodbc
# PAT with DSN
conn = pyodbc.connect(f"DSN=Databricks_PAT", autocommit=True)
cursor = conn.cursor()
cursor.execute('SELECT * FROM spark_cars')
for row in cursor.fetchall()[:5]:
    print(row)

# COMMAND ----------

connection_str_pat = (
 "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;" +
f"Host={spark.conf.get('spark.dbr_hostname')};" +
 "Port=443;" +
f"HTTPPath={spark.conf.get('spark.dbr_http_path')};" +
 "SSL=1;" +
 "ThriftTransport=2;" +
f"Schema={schema};" +
f"Catalog={catalog};" +
 "AuthMech=3;" + 
 "UID=token;" +
f"PWD={spark.conf.get('spark.dbr_token')}")

conn = pyodbc.connect(connection_str_pat, autocommit=True)
cursor = conn.cursor()
cursor.execute('SELECT * FROM spark_cars')
for row in cursor.fetchall()[:5]:
    print(row)

# COMMAND ----------

connection_str_pat

# COMMAND ----------

import pyodbc
# AAD with DSN
conn = pyodbc.connect(f"DSN=Databricks_AAD;", autocommit=True)
cursor = conn.cursor()
cursor.execute('SELECT * FROM spark_cars')
for row in cursor.fetchall()[:5]:
    print(row)

# COMMAND ----------

connection_str_aad = (
 "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;" +
f"Host={spark.conf.get('spark.dbr_hostname')};" +
 "Port=443;" +
f"HTTPPath={spark.conf.get('spark.dbr_http_path')};" +
 "SSL=1;" +
 "ThriftTransport=2;" +
f"Schema={schema};" +
f"Catalog={catalog};" +
 "AuthMech=11;" + 
 "Auth_Flow=0;" +
f"Auth_AccessToken={aad_token};")

conn = pyodbc.connect(connection_str_aad, autocommit=True)
cursor = conn.cursor()
cursor.execute('SELECT * FROM spark_cars')
for row in cursor.fetchall()[:5]:
    print(row)
