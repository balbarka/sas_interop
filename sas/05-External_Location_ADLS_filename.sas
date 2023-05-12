
options azuretenantid = "&AZ_TENANT_ID";

filename example adls "external/demo/filename/example.csv"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM";

data _null_;     
   file example;
   put 'a,b,c';
   put '10,20,30';
   put '11,21,31';
   put '12,22,32';
run;

filename class adls "external/demo/filename/class.json"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM";

proc json out=class;
   export sashelp.class;
run;
