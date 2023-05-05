/* ODBC LIBNAME with Databricks Driver */


libname odbc_dbr clear;
libname odbc_dbr odbc
   NOPROMPT="dsn=Databricks_Cluster;
             Driver=/access-clients/odbc/simba/spark/lib/64/libsparkodbc_sb64.so;
             HOST=adb-8590162618558854.14.azuredatabricks.net;
             PORT=443;
             Schema=demo;
             Catalog=sas_interop;
             SparkServerType=3;
             AuthMech=3;
             ThriftTransport=2;
             SSL=1;
             UseNativeQuery=1;"
   AUTOCOMMIT=yes;

proc contents data=odbc_dbr._all_ out=tables;
run;

libname odbc_dbr clear;
libname odbc_dbr odbc
   datasrc=Databricks_Cluster
   UID="token" 
   PWD=&DBR_TOKEN;

proc contents data=odbc_dbr._all_ out=tables;
run;



