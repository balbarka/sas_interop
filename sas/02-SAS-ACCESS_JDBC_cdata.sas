/* JDBC Connection Using the CDATA Provided Databricks Driver */
/* Not recommended due to complexity to use with Unity Catalog */
/* There is no way to set default catalog */

libname jdbc_dbr clear;

libname jdbc_dbr JDBC 
   driverclass="cdata.jdbc.databricks.DatabricksDriver"
   classpath="/opt/sas/viya/home/lib64/accessclients/jdbc"
   URL="jdbc:databricks:Server=&DBR_HOST;
        HTTPPath=&CLUSTER_PATH;Database=sas_interop_demo;
        User=token;Token=&DBR_TOKEN;
        QueryPassthrough=true;
        UseLegacyDataModel=true;";

proc sql;
DROP TABLE jdbc_dbr.jdbc_cars;
quit;

data jdbc_dbr.jdbc_cars;
set sashelp.cars;
run;

proc sql;
select * from jdbc_dbr.jdbc_cars;
quit;

/* The issue is that above uses hive_metastore */
/* We can still query catalogs using query passthrough though*/

proc sql; 
connect using jdbc_dbr; 
select * from connection to jdbc_dbr (
    SELECT * FROM sas_interop.demo.jdbc_cars);
disconnect from jdbc_dbr;
quit;

/* CAS Databricks JDBC Datasource */

CAS mySession  SESSOPTS=( CASLIB=casuser TIMEOUT=99 LOCALE="en_US" metrics=true);

caslib jdcaslib clear;
caslib jdcaslib dataSource=(srctype='jdbc',
           url="jdbc:databricks:Server=&DBR_HOST;
        HTTPPath=&CLUSTER_PATH;Database=sas_interop_demo;
        User=token;Token=&DBR_TOKEN;
        QueryPassthrough=true;"
           class="cdata.jdbc.databricks.DatabricksDriver",
           classpath="/opt/sas/viya/home/lib64/accessclients/jdbc");

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

proc fedsql sessref=mySession;
  create table cascars {options replace=true} as
  SELECT * FROM jdbc_cars;
quit;

proc casutil;
   contents casdata="cascars";
run; 

proc cas;
table.partition /                                                   
casout={caslib="jdcaslib", name="jdbc_cas_cars_part"}                             
table={caslib="jdcaslib", name="cascars", groupby={name="type"}};   
run; 
quit;

/* Works, but still uses INSERT VALUES statement*/
/* Also, still in hive_metastore which isn't ideal for UC*/
proc casutil incaslib="jdcaslib"; 
   save casdata="jdbc_cas_cars_part" outcaslib="jdcaslib" replace; 
run;

CAS mySession  TERMINATE;