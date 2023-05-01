/* This script creates and tests jdbc libnames for
     - Databricks Runtime Interactive Cluster, jdbc_dbr
     - Databricks SQL Warehouse, jdbc_wh
*/

/* Databricks Interactive cluster */

libname jdbc_dbr clear;

libname jdbc_dbr JDBC 
   driverclass="com.databricks.client.jdbc.Driver"
   classpath="/export/sas-viya/data/drivers/"
   URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
        ssl=1;AuthMech=3;httpPath=&CLUSTER_PATH;
        ConnCatalog=sas_interop;
        ConnSchema=demo;"
   user="token" 
   password=&DBR_TOKEN;

proc sql;
DROP TABLE jdbc_dbr.jdbc_cars;
quit;

data jdbc_dbr.jdbc_cars;
set sashelp.cars(obs=5);
run;

proc sql;
select * from jdbc_dbr.jdbc_cars;
quit;

/* Databricks SQL Warehouse */

libname jdbc_wh clear;

libname jdbc_wh JDBC 
   driverclass="com.databricks.client.jdbc.Driver"
   classpath="/export/sas-viya/data/drivers/"
   URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
        ssl=1;AuthMech=3;httpPath=&WAREHOUSE_PATH;
        ConnCatalog=sas_interop;
        ConnSchema=demo;"
   user="token" 
   password=&DBR_TOKEN;

proc sql;
DROP TABLE jdbc_wh.jdbc_cars;
quit;

data jdbc_wh.jdbc_cars;
set sashelp.cars(obs=5);
run;

proc sql;
select * from jdbc_wh.jdbc_cars;
quit;