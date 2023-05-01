
/* partitioned table problem */

options azuretenantid = "&AZ_TENANT_ID";

libname orc_part orc 'external/demo/cars_orc_part/'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=yes;

proc contents data=orc_part._all_ nods;
run;

proc sql outobs=5;
select * from orc_part.type=Hybrid;
quit;