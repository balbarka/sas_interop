/* SPARK CONNECT */
/* https://communities.sas.com/t5/SAS-Communities-Library/CAS-Accessing-Azure-DataBricks-SPARK-cluster/ta-p/783049 */
/* This runs off of Databricks Cluster */

CAS mySession  SESSOPTS=( CASLIB=casuser TIMEOUT=99 LOCALE="en_US" metrics=true);

caslib xcas clear;

caslib xcas dataSource=(srctype='spark',
           url="jdbc:databricks://&DBR_HOST:443/default;transportMode=http;ssl=1;httpPath=&CLUSTER_PATH;AuthMech=3;UID=token;PWD=&DBR_TOKEN"
           driverclass="com.databricks.client.jdbc.Driver",
           classpath="/export/sas-viya/data/drivers/",
           BULKLOAD=NO,
           schema="default");

/* Save CAS table to Databricks database */
proc casutil outcaslib="xcas" incaslib="xcas";
load data=sashelp.cars casout="cars" replace;
save casdata="cars" casout="cars_sas"  replace;
list files;
quit;

CAS mySession  TERMINATE;