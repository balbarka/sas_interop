
options azuretenantid = "&AZ_TENANT_ID";

filename out adls "example.json"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM";

proc json out=out;
   export sashelp.class;
run;

data _null_;     
   file out;
   put 'line 1';
run;

data _null_;     
   file out;
   put 'line 1';
run;
