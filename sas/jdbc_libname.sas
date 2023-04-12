/* This script runs off sql warehouse or interactive cluster depending on HTTP_PATH*/

* HTTP_PATH=WAREHOUSE_PATH;
%let HTTP_PATH=&CLUSTER_PATH;

libname jdbc clear;

libname jdbc JDBC 
   driverclass="com.databricks.client.jdbc.Driver"
   classpath="/export/sas-viya/data/drivers/"
   URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
        ssl=1;AuthMech=3;httpPath=&HTTP_PATH;
        ConnCatalog=sas_interop;
        ConnSchema=demo;"
   user="token" 
   password=&DBR_TOKEN;

proc sql;
DROP TABLE jdbc.jdbc_cars;
quit;

data jdbc.jdbc_cars;
set sashelp.cars(obs=5);
run;

proc sql;
select * from jdbc.jdbc_cars;
quit;
