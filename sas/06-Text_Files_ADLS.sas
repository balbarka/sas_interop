/* Multiple Methods for Exporting Data to ADLS:
    - CSV File to table
    - TXT File to table
    - CSV Files to partitioned table
*/

options azuretenantid = "&AZ_TENANT_ID";

data cars;
set sashelp.cars;
format MSRP Invoice _NUMERIC_;
run;

/* CSV Example, overwrite, non-partitioned */

filename cars_csv adls
   "external/demo/export_cars_csv/export_cars.csv"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM"
   encoding="utf-8";

proc export data=cars
   outfile=cars_csv
   dbms=dlm replace;
   delimiter=',';
run;

proc sql;
select * from jdbc_dbr.export_cars_csv;
quit;

/* TXT Example, overwrite, non-partitioned */

filename cars_txt adls
   "external/demo/export_cars_txt/export_cars.txt"
   applicationid="&ADLS_APPLICATION_ID"
   accountname="&ADLS_ACCOUNT_NAME"
   filesystem="&ADLS_FILESYSTEM";


proc export data=cars
   outfile=cars_txt
   dbms=TAB replace;
run;

proc sql;
select * from jdbc_dbr.export_cars_csv;
quit;

/* TODO: Write example that writes types into partitioned table */
/* https://blogs.sas.com/content/iml/2011/09/07/loops-in-sas.html */