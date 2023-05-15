/* Methods for writing ORC files to ADLS */

options azuretenantid = "&AZ_TENANT_ID";

/* Multiple Data Source files with different schemas in single folder */

libname orc_mixd orc 'external/demo/orc_mixd'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

data orc_mixd.cars2;
set sashelp.cars;
run;

data orc_mixd.class;
set sashelp.class;
run;

proc contents data=orc_mixd._all_ nods;
run;

/* Single Data Source file per subfolder (separate read / write libnames) */

libname orc_cars orc 'external/demo/orc_dad/cars'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

data orc_cars.cars;
set sashelp.cars;
run;

options azuretenantid = "&AZ_TENANT_ID";
libname orc_cls orc 'external/demo/orc_dad/class'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

data orc_cls.class;
set sashelp.class;
run;

libname orc_dad orc 'external/demo/orc_dad'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=yes;

proc sql;
select * from orc_dad.cars;
quit;

proc contents data=orc_dad._all_ nods;
run;

/* Single Data Source file per subfolder (separate read / write libnames) */

%macro write_carType(carType);

libname adls_orc orc "external/demo/orc_cars_part/type=&carType"
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

libname orc_part orc "external/demo/orc_cars_part"
    storage_account_name = "&ADLS_ACCOUNT_NAME"
    storage_application_id = "&ADLS_APPLICATION_ID"
    storage_file_system = "&ADLS_FILESYSTEM"
    directories_as_data=yes;

proc contents data=orc_part._all_ nods;
run;

proc sql outobs=5;
select * from orc_part.type=Hybrid;
quit;

/* Multiple Data Source files with same schemas in single folder (Non-Partitioned ORC Table) */

libname orc_np orc "external/demo/orc_cars_nonpart"
    storage_account_name = "&ADLS_ACCOUNT_NAME"
    storage_application_id = "&ADLS_APPLICATION_ID"
    storage_file_system = "&ADLS_FILESYSTEM";

%macro write_carDriveTrain(carDriveTrain);
data orc_np.cars_&carDriveTrain;                            
    set sashelp.cars;                             
    where DriveTrain="&carDriveTrain";
run;
%mend write_carDriveTrain;

%write_carDriveTrain(Front)
%write_carDriveTrain(All)
%write_carDriveTrain(Rear)

proc contents data=orc_np._all_ nods;
run;

proc sql outobs=5;
select * from orc_np.cars_Rear;
quit;

/* CAS ORC - Show writing large files from CAS */

/* Create a CAS session with caslib with cars */
CAS mySession
    SESSOPTS=(azureTenantId="&AZ_TENANT_ID");

proc cas;
  session mySession;
  addcaslib /
  datasource={srctype="adls"
               accountName="&ADLS_ACCOUNT_NAME"
               filesystem="&ADLS_FILESYSTEM"
               applicationId="&ADLS_APPLICATION_ID"
              }
   name="adlscas"
   subdirs=true
   path="external/demo/orc_cas_large";
run;
quit;

/* This will assign a libname to adlscas */
CASLIB _ALL_ ASSIGN SESSREF=mySession;
* libname adlscas cas sessref=mySession;

/* We now have an in-memory cars table in adlscas */
/* not used, just to demo to memory table from other libnames*/
data adlscas.cars;            
   set sashelp.cars;
run;

/* Create a large in-memory table */
data adlscas.large;
array vars(300) $8 x1-x300;
do j=1 to 500000;
id=put(rand('integer',1,4),8.);
do i=1 to 300;
vars(i)=rand("Uniform");
end;
output;
end;
drop i j;
run;


%macro save_largePart(carPart);
proc fedsql SESSREF=mySession;
    CREATE TABLE adlscas.large_&&carPart
    AS SELECT * FROM adlscas.large
    WHERE id = &carPart;
quit;
proc cas;
table.save /
   caslib="adlscas"
   table="large_&&carPart"
   name="large_&&carPart"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;
%mend save_largePart;

%MACRO save_loop;
%DO p = 1 %TO 4;
 %save_largePart(&p);
%END;
%mend save_loop;
%save_loop

/* It may be possible to run the above parallel distributed, but
 would need to find reference code on how to do */