# sas_interop

There are many ways to create interoperablity between SAS Viya and Databricks. While some of these are explicitily provided in documentation as a 
product features, there are a lot of interoperablity patterns that should be considered that are not explicitly identified in documentation. This
repo provides examples of options available and identify important feature / version considerations.

These examples were developed in the following environements:
 * [Azure SAS® Viya® (Pay-As-You-Go)](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/sas-institute-560503.sas-viya-on-azure)
 * [Databricks Runtime 12.2 LTS](https://learn.microsoft.com/en-us/azure/databricks/runtime/dbr)

While there is reference code provided as SAS files and Jupyter notebooks, the interoperabily methods can all be inspected from Databricks notebooks.
The notebooks are provided in order which can optionally be a way to go through all the material.

SAS Interoperability

| cloud | method /</br> interface | description | Notebooks |
| ----- | ----------------------- | ----------- | --------- |
| az    | [SASPy](https://github.com/sassoftware/saspy) | Python API for SAS, SAS maintainined | [01-SASPy_SETUP.py](./databricks/01-SASPy_SETUP.py) </br> [01-SASPy.py](./databricks/01-SASPy.py) |
| az    | [SAS/ACCESS Interface to JDBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1usgr00wc9cvln1gnyp1807qu17.htm) | Library and Dataset methods using JDBC | [02-SAS-ACCESS_JDBC.py](./databricks/02-SAS-ACCESS_JDBC.py) |
| az    | [SAS/ACCESS Interface to ODBC](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/p1g72kbb0m01y1n1gm1lh532n5ru.htm) | Library and Dataset methods using ODBC | [03-SAS-ACCESS_ODBC.py](./databricks/03-SAS-ACCESS_ODBC.py) |
| az    | [SAS/ACCESS Interface to Spark](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/acreldb/n1mfknm1ehokoun1k11wloqtump3.htm#p1artpb4afn43qn1s0kww6dqysi0) | Library and Dataset methods using ODBC | [04-SAS-ACCESS_Spark.py](./databricks/04-SAS-ACCESS_Spark.py) |
| az    | [Azure Access Method](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/lestmtsglobal/n0yc4ac0hf1yefn1r504kw2uesiw.htm) </br> [Databricks External Location](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-external-locations) | Create a single shared data source in ADLS accessed directly by SAS & Databricks | [05-External_Location_ADLS.py](./databricks/05-External_Location_ADLS.py) |
| az    | [Delimited Files](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/casref/n0hsybxtdnp6htn1i523xn5pl9n7.htm) | Use shared ADLS location to read/write text files | [06-Text_FIles_ADLS.py](./databricks/06-Text_FIles_ADLS.py) |
| az    | [ORC Engine](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/enghdff/n1ginf51kejgc4n1os12lv59d81a.htm) | Use shared ADLS location to read/write ORC files | [07-ORC_ADLS.py](./databricks/07-ORC_ADLS.py) |
| az    | [Parquet Engine](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/casref/p0u5p2nvqu04gfn1w3zaohdfcoys.htm) | Use shared ADLS location to read/write Parquet files | [08-Parquet_ADLS.py](./databricks/08-Parquet_ADLS.py) |
| az    | [Export sas7bdat](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/caspg/p1xt9526uq5etwn1vmnk8koh0k6y.htm#p1likrphj5ez6gn1ospia7xtdogv) </br> [spark-sas7bdat](https://github.com/saurfang/spark-sas7bdat) </br> [pandas.read_sas](https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html#pandas-read-sas) | Use shared ADLS location to read/write SAS files | [09-sas7bdat_ADLS.py](./databricks/09-sas7bdat_ADLS.py) |

## SAS Documentation References:
Since there are many deployment patterns and documentation sources for SAS, it is important
that the documentation specific to your deployment is used. These interoperability examples were
developed using an [Azure SAS Viya Pay-As-You-Go](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/sas-institute-560503.sas-viya-on-azure)
managed application. As of 19 APR 2023, the current SAS Viya Version used by the managed application
was **2023.01**, thus the appropriate documentation version used during the development of these
interoperability patterns are:

 * [SAS® Viya® Platform Administration (2023.01)](https://documentation.sas.com/doc/en/sasadmincdc/v_036/sasadminwlcm/home.htm)
 * [SAS® Viya® Platform Operations (2023.01)](https://documentation.sas.com/doc/en/itopscdc/v_036/itopswlcm/home.htm)
 * [Programming Documentation for the SAS® Viya® Platform (2023.01)](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/pgmsaswlcm/titlepage.htm)

## Potential Future Examples:
 * [SAS Embedded Process for Spark Action (Databricks)](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/casanpg/p1qssgxdy96qoin1fdzjvjxg0r4b.htm)
 * Provide examples that leverage S3 instead of ADLS
 * [Databricks SQL Connector](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector) - included for consideration since the SAS Viya PAYG environmenet includes jupyter and SAS allows for [proc python](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n13mxnb160s5m9n1r7y0z6ixsjoc.htm)