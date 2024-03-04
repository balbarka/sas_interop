# AutoExec Config

For many of the examples in the sas_interop notebooks, we are using macro variables. Specifically, macro variables like:
``` SAS
%let DBR_HOST=...;
%let DBR_TOKEN=...;
%let CLUSTER_PATH=...;
%let WAREHOUSE_PATH=...;

%let AZ_TENANT_ID=...;
%let ADLS_APPLICATION_ID=...;
%let ADLS_ACCOUNT_NAME=...;
%let ADLS_FILESYSTEM=...;
```

You can get to this setting for all SAS Job Execution Computes by navigating as:
 - Top Left Corner Menu of SAS Studio
 - Select ADMINISTRATION > Manage Environment
 - Select Contexts (Looks like a wrench infront of a database)
 - Change your view to Conpute Contexts
 - Select SAS Job Execution compute context
 - Edit -> Advanced
 - Write the above varaibles into the auto exec statement

**NOTE**: This appraoch is used for ease of demo. For production configurations, please use SAS approved secret handling methods.

**NOTE**: There is also another configuration that is helpful to set here. It is the `azureauthcacheloc`. This happens to not be the default location when using some distributions of the SAS Viya Pay-Go Azure marketplace installation. Thus, we will set the SAS option in the same advanced tab, but options section the following:
```SAS
-azureauthcacheloc "/export/sas-viya/data"
```

