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
* CASLIB _ALL_ ASSIGN SESSREF=mySession;
libname adlscas cas sessref=mySession;

/* We now have an in-memory cars table in adlscas */
/* not used, just to demo to memory table from other libnames*/
data adlscas.cars;            
   set sashelp.cars;
run;

/* Create a large in-memory table */
data adlscas.large;
array vars(300) $8 x1-x300;
do j=1 to 5000000;
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
%DO p = 1 %TO 10;
 %save_largePart(&p);
%END;
%mend save_loop;
%save_loop


%let carPart = 1;



proc fedsql;
   create table mybase.outtable as
      select product.prodid, product.product, customer.name,
         sales.totals, sales.country
      from mybase.product, mybase.sales, mybase.customer
      where product.prodid = sales.prodid and 
         customer.custid = sales.custid;
   select * from mybase.outtable;
quit;



%macro write_carPart(carPart);

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












proc cas;
table.partition /                                                   
casout={caslib="adlscas", name="largep"}                             
table={caslib="adlscas", name="large", groupby={name="id"}};   
run;
quit;

proc cas;
table.save /
   caslib="adlscas"
   table="largep"
   name="largepo"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;

proc fedsql sessref=mySession;
    select * from adlscas.large LIMIT 5;
quit;



libref xxx cas;

caslib _all_ assign; 

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
   path="external/export/demo/xxx";
run;

table.loadtable;
run;

table.save /
   caslib="adlscas"
   table="cars"
   name="ORCsave"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;

proc cas;
addcaslib /
   datasource={srctype="adls"
               accountName="&ADLS_ACCOUNT_NAME"
               filesystem="&ADLS_FILESYSTEM"
               applicationId="&ADLS_APPLICATION_ID"
              }
   name="adlscas3"
   subdirs=true
   path="external/export/demo/xxx";
run;
table.save /
   caslib="adlsdata3"
   table="outtable"
   name="caswrite"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;

proc casutil;
	 load data=sashelp.cars outcaslib="adlscas"
     casout="cars";
run;

proc fedsql SESSREF=mySession;
   create table adlscas.outtable as select * from cars;
quit;

proc casutil outcaslib="adlscas";
   list tables;
quit;

proc cas;
table.save /
   caslib="adlsdata"
   table="outtable"
   name="caswrite"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;



proc cas;
table.save /
table={name="cars" caslib="adlscas"}
name="xxx" caslib="adlscas" replace="yes";
run;

proc casutil SESSREF=mySession;
   libs=(adlscas);
   load data=adlscas.OUTTABLE promote;                                     /* 2 */  
   contents casdata="outer";
run; 

caslib _all_ assign; 

/* PROC FedSQL code */
proc fedsql sessref=mySession; 
   SELECT * FROM casuser.mycars;
quit;

proc casutil outcaslib="casuser";                      /* 2 */
   promote casdata="cars";
quit;


/* PROC FedSQL code */
proc fedsql sessref=mySession; 
   SELECT * FROM casuser.mycars;
quit;

proc cas;
table.save /
   caslib="adlscas"
   table="cars"
   name="ORCsave"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;

proc casutil incaslib='casuser';                          /* 3 */ 
   save casdata='cars' outcaslib='adlscas' replace; 
   list; 
run;  




caslib _all_ assign; 




proc casutil;
	 load data=sashelp.cars outcaslib="casuser"
     casout="cars";
run;

caslib _all_ assign; 

libname mycas cas;                       
caslib _all_ assign; 
data mycas.mycars;
   set sashelp.cars;
run;



/* Create an ADLS CASLIB */
options azuretenantid = "&AZ_TENANT_ID";
caslib azds datasource=(
      srctype="adls"
      accountname="&ADLS_ACCOUNT_NAME"
      filesystem="&ADLS_FILESYSTEM"
      applicationId="&ADLS_APPLICATION_ID"
   )
   subdirs;

proc casutil;
   list tables;
quit;



proc cas;
addcaslib /
   datasource={srctype="adls"
               accountName="&ADLS_ACCOUNT_NAME"
               filesystem="&ADLS_FILESYSTEM"
               applicationId="&ADLS_APPLICATION_ID"
              }
   name="adlscas"
   subdirs=true
   path="export/demo/cas";
run;
quit;

proc casutil;
	 load data=sashelp.cars outcaslib="adlscas"
     casout="cars";
run;

proc cas;
table.save /
   caslib="adlsdata"
   table="cars"
   name="ORCsave"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;




libname orc_mixd orc 'external/demo/orc_mixd'
   storage_account_name = "&ADLS_ACCOUNT_NAME"
   storage_application_id = "&ADLS_APPLICATION_ID"
   storage_file_system = "&ADLS_FILESYSTEM"
   directories_as_data=no;

proc casutil;
   list files;
quit;
cas casauto sessopts=(azureTenantId='b1c24e5c-3625-4593-a430-9552373a0c2f');

caslib azds datasource=(
      srctype="adls"
      accountname="myaccount"
      filesystem="dir"
      applicationId="b1fc955d5c-e0e2-45b3-a3cc-a1cf54120f"
   )
   subdirs;

proc casutil;
   list files;
quit;





options azuretenantid = "&AZ_TENANT_ID";

/* Multiple DATA-files per folder */


proc cas;
addcaslib /
   datasource={srctype="adls"
               accountName="&ADLS_ACCOUNT_NAME"
               filesystem="&ADLS_FILESYSTEM"
              }
   name="adlsdata"
   subdirs=true
   path="export/demo/cas";
run;

table.save /
   caslib="adlsdata"
   table="xyz"
   name="ORCsave"
   replace=True
   exportoptions={filetype="orc"};
run;
quit;





