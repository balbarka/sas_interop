/* LIBNAME for Databricks Interactive Cluster */

libname jdbc_dbr clear;

libname jdbc_dbr JDBC 
   driverclass="com.databricks.client.jdbc.Driver"
   classpath="/access-clients/jdbc"
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
set sashelp.cars;
run;

proc sql;
select * from jdbc_dbr.jdbc_cars;
quit;

/* LIBNAME for Databrick SQL Warehouse */
/* Will spin up WH if not already started */

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
set sashelp.cars;
run;

proc sql;
select * from jdbc_wh.jdbc_cars;
quit;

/* Passthrough Example with PIVOT */

/* Run a PIVOT using proc tabulate */

proc tabulate data=jdbc_dbr.jdbc_cars;
class drivetrain type;
var horsepower;
tables type,drivetrain*horsepower*(mean);
run;

/* Run a PIVOT using Passthrough */

proc sql; 
connect using jdbc_dbr; 
select * from connection to jdbc_dbr (
    SELECT type,  all_hp, front_hp, rear_hp
    FROM (SELECT type, drivetrain, horsepower FROM jdbc_cars)
    PIVOT (ROUND(AVG(horsepower),2) AS horsepower
           FOR drivetrain
           IN ("Front" AS front_hp, "All" AS all_hp, "Rear" AS rear_hp)))
    ORDER BY type;     
quit;

/* Databricks JDBC CAS Connector*/

CAS mySession  SESSOPTS=( CASLIB=casuser TIMEOUT=99 LOCALE="en_US" metrics=true);

caslib jdcaslib clear;
caslib jdcaslib dataSource=(srctype='jdbc',
           url="jdbc:databricks://&DBR_HOST:443;
                transportMode=http;ssl=1;httpPath=&CLUSTER_PATH;
                AuthMech=3;UID=token;PWD=&DBR_TOKEN;
                ConnCatalog=sas_interop;ConnSchema=demo;"
           class="com.databricks.client.jdbc.Driver",
           classpath="/export/sas-viya/data/drivers/");


proc casutil;
   contents casdata="jdbc_cars";
run; 


proc fedsql sessref=mySession;
  create table cascars {options replace=true} as
  SELECT * FROM jdbc_cars;
quit;

proc casutil;
   contents casdata="cascars";
run; 

proc cas;
table.partition /                                                   
casout={caslib="jdcaslib", name="cascars_part"}                             
table={caslib="jdcaslib", name="cascars", groupby={name="type"}};   
run; 
quit;

proc casutil;
   contents casdata="cascars_part";
run; 

/* This fails, isues with CREATE TABLE syntax from Driver */
/* Filed ES Ticket */
/* Still not viable process since would have ran INSERT VALUES statement*/
proc casutil incaslib="jdcaslib"; 
   save casdata="cascars_part" outcaslib="jdcaslib" ; 
run;

CAS mySession  TERMINATE;


