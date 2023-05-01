data cars_temp;
set Sashelp.cars;
run;

proc print data=cars_temp(obs=10);
var Model Type Origin HorsePower MSRP;
run;
