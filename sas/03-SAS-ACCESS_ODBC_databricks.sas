/* TODO */

libname dbx_odbc clear;

libname dbx_odbc odbc
    datasrc=Databricks
    user=token
    password=&DBR_TOKEN;

libname dbx_odbc odbc datasrc=Databricks_Cluster;




proc sql;
select * from dbx_odbc.jdbc_cars;
quit;

export DFESP_HOME=xxx;


x "setenv STATUS 'running dstep doit'";
  data getenv;
  homedir=sysget('HOME');
  put homedir=;
  run;

x "setenv STATUS 'running dstep doit'";
data doit;
run;

data getenv;
homedir=sysget('HOME');
put homedir=;
  run;


x "export STATUS='TESTING'";

data getstat;
stat=sysget('LD_LIBRARY_PATH');
put stat=;
run;