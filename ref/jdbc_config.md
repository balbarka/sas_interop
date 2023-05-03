


There is an issue with the documentation that we are not able to get to the default path within SAS Studio. Looks like there is another step necessary to add this folder to path in k8s. Until able to figure out, we will be using the SAS Studio Path /export/sas-viya/data/drivers - which is not consistant with SAS Documentation.

We are going to save our databricks jdbc driver in:

/data-drivers/jdbc this is the recommended location based upon [Shared File System Recommended Directory Structure](https://go.documentation.sas.com/doc/en/itopscdc/v_036/itopssr/n0ampbltwqgkjkn1j3qogztsbbu0.htm#p0u8ihdebannnxn1oe7fh89kavwj)

From within the jump server:

````
mkdir -p /viya-share/sas-viya/data-drivers/jdbc
cd /viya-share/sas-viya/data-drivers/jdbc


wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.32/DatabricksJDBC42-2.6.32.1054.zip

# sudo apt-get install unzip


unzip DatabricksJDBC42-2.6.32.1054.zip

```



access-clients