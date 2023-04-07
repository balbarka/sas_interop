/* This script runs off sql warehouse */

libname x clear;

libname x JDBC driverclass="com.databricks.client.jdbc.Driver"
   URL="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/60da312dbfd48551;" user="token" 
   password="dapi059bbc61352dd35f41cb44225670688a-2" classpath="/export/sas-viya/data/drivers/";


/*
data x.cars;
set sashelp.cars(obs=5);
run; */

proc sql;
select * from x.cars;
quit;

