/* This script runs off sql warehouse */

libname jdbc_wh clear;

libname jdbc_wh JDBC 
   driverclass="com.databricks.client.jdbc.Driver"
   classpath="/export/sas-viya/data/drivers/"
   URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;
        ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/60da312dbfd48551;
        ConnCatalog=sas_interop;
        ConnSchema=demo;"
   user="token" 
   password="dapi059bbc61352dd35f41cb44225670688a-2";

proc sql;
DROP TABLE jdbc_wh.jdbc_wh_cars;
quit;

data jdbc_c.jdbc_wh_cars;
set sashelp.cars(obs=5);
run;

proc sql;
select * from jdbc_wh.jdbc_wh_cars;
quit;
