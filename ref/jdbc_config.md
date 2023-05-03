# Databricks JDBC Driver Configuration

We must complete the following configuration before being able to use the [Databricks JDBC Driver](https://www.databricks.com/spark/jdbc-drivers-download).

 1) Goto the [Databricks JDBC Driver Download](https://www.databricks.com/spark/jdbc-drivers-download) page and get a copy of the drive. Unzip.
 2) scp the driver to the jump server, then scp the driver to the nfs server.
 3) Move to final path in nfs server, `/export/access-clients/jdbc`

Alternately, in nfs server as root:
``` bash
cd /export/access-clients/jdbc

wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.32/DatabricksJDBC42-2.6.32.1054.zip

unzip DatabricksJDBC42-2.6.32.1054.zip
```

**NOTE**: Although you save to the path, `/export/access-clients/jdbc` on the nfs server, this is actually mounted to `/access-clients/jdbc` in SAS Studio. Thus, your **classpath** to use this jar will be `classpath=/access-clients/jdbc`.

**NOTE**: Provided with SAS Viya is the [CData Databricks driver](https://cdn.cdata.com/help/LKH/jdbc/). It is located in the default SAS Studio classpath, `/opt/sas/viya/home/lib64/accessclients/jdbc` and therefore you do not need to provide the classpath when working with the CData Databricsk Driver.

**NOTE**: `/export/access-clients/jdbc` is the most logical location when you look at the mounted fs. However, the documented location, `data-drivers/jdbc`, in [LIBNAME Statement for JDBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n0zm6fjwtgsnrzn1fegvyhl3yrwd.htm) doesn't exist.
