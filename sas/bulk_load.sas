options azuretenantid= "tenant-id";

libname spkdbazd spark driverClass="com.databricks.client.jdbc.Driver"
  bulkload=yes
  bl_applicationid='application-id'
  bl_accountname='account-name'
  bl_filesystem='filesystem-name'
  <bl_folder='mypath'>
  url='jdbc:spark://server:port/schema; transportMode=http;ssl=1;HTTPPath=myHttpPath;
  AuthMech=3;defaultStringColumnLength=255;useNativeQuery=1;<options>'
  user=token
  password=mytoken