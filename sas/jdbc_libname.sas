libname x JDBC driverclass="com.databricks.client.jdbc.Driver"
   URL="jdbc:databricks://adb-8590162618558854.14.azuredatabricks.net:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/60da312dbfd48551;" user="token" 
   password="xxx" classpath="/export/sas-viya/data/drivers/";


data x.cars;
set sashelp.cars(obs=5);
run;

proc sql;
select * from x.cars;
quit;