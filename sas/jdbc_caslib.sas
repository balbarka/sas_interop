%let HOSTNAME="adb-8590162618558854.14.azuredatabricks.net";
%let UID=token;
%let PWD="xxx";
%let HTTP_PATH="/sql/1.0/warehouses/60da312dbfd48551";

caslib clibDatabricks sessref=mysess
 datasource=(srctype="jdbc",
             classpath="/Files/data/drivers",
             driverclass="com.databricks.client.jdbc.Driver",
             URL="jdbc:databricks://adb-8590162618558854.14.azuredatabricks.net:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/60da312dbfd48551;",
             username="token",
             password="xxx",
             schema=“default“);
