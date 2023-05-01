/* Methods for writing ORC files to ADLS */

options azuretenantid = "&AZ_TENANT_ID";

/* Multiple DATA-files per folder */

libname orc_mixd orc 'external/demo/orc_mixd'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

data orc_mixd.cars;
set sashelp.cars;
run;

data orc_mixd.class;
set sashelp.class;
run;

proc contents data=orc_mixd._all_ nods;
run;

/* Single DATA-file per folder */

libname orc_cars orc 'external/demo/orc_dad/cars'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

data orc_cars.cars;
set sashelp.cars;
run;

libname orc_dad orc 'external/demo/orc_dad'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=yes;

proc sql;
select * from orc_dad.cars;
quit;

proc sql;
select * from jdbc_dbr.cars_orc;
quit;

/* Code to write to cars partitioned by type */

%macro write_carType(carType);

libname adls_orc orc "external/demo/cars_orc_part/type=&carType"
    storage_account_name = "&ADLS_ACCOUNT_NAME"
    storage_application_id = "&ADLS_APPLICATION_ID"
    storage_file_system = "&ADLS_FILESYSTEM";

data adls_orc.orc_cars;                            
    set sashelp.cars;                             
    where Type="&carType";
run;
%mend write_carType;

%write_carType(Sports)
%write_carType(SUV)
%write_carType(Sedan)
%write_carType(Hybrid)
%write_carType(Truck)
%write_carType(Wagon)


options azuretenantid = "&AZ_TENANT_ID";

libname adls_orc orc "external/demo/orc"
    storage_account_name = "&ADLS_ACCOUNT_NAME"
    storage_application_id = "&ADLS_APPLICATION_ID"
    storage_file_system = "&ADLS_FILESYSTEM"
    directories_as_data=yes;


proc sql;
select * from adls_orc.cars_orc;
quit;

proc print data=adls_orc.cars_orc noobs; /*3*/
run;