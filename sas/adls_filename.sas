
%let AZ_TENANT_ID=9f37a392-f0ae-4280-9796-f1864a10effc;
%let ADLS_APPLICATION_ID=0fc5c2b0-cfcf-4787-bc59-406368455a5a;
%let ADLS_ACCOUNT_NAME=hlsfieldexternal;
%let ADLS_FILESYSTEM=sas-interop;


options azuretenantid = "&AZ_TENANT_ID";

filename out adls "example.txt"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM";

data _null_;     
   file out;
   put 'line 1';
run;

data _null_;     
   file out;
   put 'line 1';
run;
