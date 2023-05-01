/* https://communities.sas.com/t5/SAS-Communities-Library/Accessing-Azure-Databricks-with-CData-driver/ta-p/835440 */ 


%let MYDRIVERCLASS="cdata.jdbc.databricks.DatabricksDriver";

%let MYURL="jdbc:databricks:Server=&DBR_HOST;
            HTTPPath=&CLUSTER_PATH;Database=demo;Catalog=sas_interop;
            QueryPassthrough=true;Token=&DBR_TOKEN" ;

libname CdtSpark clear;
libname CdtSpark spark
    driverClass=&MYDRIVERCLASS
    bulkload=NO
    url="jdbc:databricks:Server=&DBR_HOST;
         HTTPPath=&CLUSTER_PATH;Database=default;
         QueryPassthrough=true;Token=&DBR_TOKEN" ;


proc sql;
select * from CdtSpark.cars;
quit;

libname CdtSpark clear;
libname CdtSpark spark
    driverClass=&MYDRIVERCLASS
    bulkload=YES
    bl_applicationid='&ADLS_APPLICATION_ID'
    bl_accountname='&ADLS_ACCOUNT_NAME'
    bl_filesystem='&ADLS_FILESYSTEM'
    url="jdbc:databricks:Server=&DBR_HOST;
         HTTPPath=&CLUSTER_PATH;Database=default;
         QueryPassthrough=true;Token=&DBR_TOKEN" ;

data CdtSpark.spark_cars2;
set sashelp.cars(obs=5);
run;

proc sql;
select * from CdtSpark.cars;
quit;

