# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SASPy Example Notebook
# MAGIC
# MAGIC This SASPy Demo notebook makes use of example commands from [saspy_example_github.ipynb](https://github.com/sassoftware/saspy-examples/blob/main/SAS_contrib/saspy_example_github.ipynb).
# MAGIC
# MAGIC SASPy is a SAS maintained python library available on [PyPI.org](https://pypi.org/project/saspy/) with [documentation](https://sassoftware.github.io/saspy/).
# MAGIC
# MAGIC This notebook is also expects to have automatic configuration of SAS environement done via statup scripts. See <a href="$./01-SASPy_SETUP" target="_blank">01-SASPy_SETUP</a> for configuration details.
# MAGIC
# MAGIC The configuration startup script provides three settings automatically:
# MAGIC  - `saspy` library already imported with configurations for remote environement.
# MAGIC  - `SAS` line and cell magic command to be able to call SAS directly in cell without using SAS syntax (without need for string escapes)
# MAGIC  - `SAS_file` line magic command to be able to execute a local \*.sas file in a remote SAS environment 
# MAGIC  
# MAGIC  ---
# MAGIC  
# MAGIC ## Confirm SAS Config is complete
# MAGIC
# MAGIC We'll list our SAS configs showing are are infact using a `sascfg_personal.py` file. Then we will instantiate a [SASsession](https://sassoftware.github.io/saspy/api.html#saspy.SASsession) which we will use to demonstrate the class features.
# MAGIC

# COMMAND ----------

print(saspy.list_configs())
sas = saspy.SASsession(results='HTML')
sas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can now create a [SASData](https://sassoftware.github.io/saspy/api.html#saspy.sasdata.SASdata) object, cars, and run commands against it:

# COMMAND ----------

# DBTITLE 0,Create a SASdata object to use to access the cars data set in the sashelp library.
cars = sas.sasdata('cars', libref='sashelp')
cars.heatmap('msrp','horsepower')

# COMMAND ----------

cars.tail()

# COMMAND ----------

for col in ['horsepower','MPG_City', 'MSRP']:
    cars.hist(col, title='Histogram showing '+col.upper())

# COMMAND ----------

# built in python help exists for SASPy as well:
cars.hist?

# COMMAND ----------

# You can get a list of assigned libraries within your session:
for libref in sas.assigned_librefs():
    print(libref)

# COMMAND ----------

# You see a libref SASUSER... What's in SASUSER?
sas.datasets('sasuser')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The SASsession object has a [submit](https://sassoftware.github.io/saspy/api.html#saspy.SASsession.submit) method to submit any SAS code you want. This is the basis that we use for the %SAS Magic in <a href="$./01-SASPy_SETUP" target="_blank">01-SASPy_SETUP</a>.
# MAGIC
# MAGIC The [submit](https://sassoftware.github.io/saspy/api.html#saspy.SASsession.submit) method returns a dictionary with the LOG and the LST.
# MAGIC You can print the log and sas.HTML the results from the Listing.
# MAGIC You can also prompt for macro variable substitution at runtime!

# COMMAND ----------

ll = sas.submit('data &dsname; user="&user"; hidden="&pw"; run; proc print data=&dsname;run;', prompt={'user': False, 'pw': True, 'dsname': False})

# COMMAND ----------

sas.HTML(ll['LST'])

# COMMAND ----------

# MAGIC %md
# MAGIC This is powerful becauase it allows a way to be able enter secrets into a notebook without risk of the secret revealed in the notebooks output or SAS Logs:

# COMMAND ----------

print(ll['LOG'])

# COMMAND ----------

# MAGIC %md
# MAGIC You can also use the [submitLOG](https://sassoftware.github.io/saspy/api.html#saspy.SASsession.submitLOG) method to have the LOG from the submitted code rendered for you:

# COMMAND ----------

sas.submitLOG('libname work list;')

# COMMAND ----------

# MAGIC %md
# MAGIC Use the [submitLST](https://sassoftware.github.io/saspy/api.html#saspy.SASsession.submitLST) method to have the LST rendered for you automatically. You can also add an option to get LST, unless there isn't any, then the LOG, or both LST and LOG, in either order.

# COMMAND ----------

sas.submitLST('data a;x=1;run; proc print data=a;run;')

# COMMAND ----------

sas.submitLST('data a;x=1;run; proc print data=a;run;', method='listorlog')

# COMMAND ----------

sas.submitLST('data a;x=1;run; proc print data=NoA;run;', method='listorlog')

# COMMAND ----------

sas.submitLST('data a;x=1;run; proc print data=a;run;', method='listandlog')

# COMMAND ----------

sas.submitLST('data a;x=1;run; proc print data=a;run;', method='logandlist')

# COMMAND ----------

# MAGIC %md
# MAGIC There are also methods to be able to read CSV directly from the SAS OS file system:

# COMMAND ----------

cars.to_csv('/tmp/cars.csv')

# COMMAND ----------

carscsv = sas.read_csv('/tmp/cars.csv', 'cars_cvs')
carscsv.tail(7)

# COMMAND ----------

 carscsv.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can also transfer data between SAS Datasets and Pandas DataFrames:

# COMMAND ----------

import pandas
car_df = cars.to_df()
car_df.head()

# COMMAND ----------

car_df.dtypes

# COMMAND ----------

cars.columnInfo()

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas DataFrames [describe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html) method is similar to [Proc Means](https://go.documentation.sas.com/doc/en/pgmsascdc/v_035/proc/n0k7qr5c2ah3stn10g1lr5oytz57.htm). SASdata object has the [describe](https://sassoftware.github.io/saspy/api.html#saspy.sasdata.SASdata.describe) method (and means as an alias method).
# MAGIC

# COMMAND ----------

car_df.describe()

# COMMAND ----------

cars.means()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here is an example of the full roud trip back to a SAS Dataset:

# COMMAND ----------

cars_full_circle = sas.df2sd(car_df, 'cfc')
cars_full_circle.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There is support for a [SASstat](https://sassoftware.github.io/saspy/api.html#saspy.sasdata.SASdata.describe). Let's run a linear regression and look at our options:

# COMMAND ----------

cars

# COMMAND ----------

# MAGIC %%SAS
# MAGIC proc reg data=sashelp.cars;
# MAGIC     model Horsepower = Cylinders EngineSize;
# MAGIC run;

# COMMAND ----------

xxx = sas.sasstat()

# COMMAND ----------

?xxx.reg

# COMMAND ----------

#TODO: check why not rendering
sasstat = sas.sasstat()
stat_results = sasstat.reg(data=cars, model='Horsepower = Cylinders EngineSize')
dir(stat_results)

# COMMAND ----------

# We can then also inspect all the results:
stat_results.ALL()

# COMMAND ----------

# MAGIC %md
# MAGIC You can also run proc sql for any datasource you want (even Databricks, Hadoop, etc.). We'll write our proc sql statement in %SAS magic to improve readability.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC proc sql;
# MAGIC     create table sales
# MAGIC     as select
# MAGIC         month,
# MAGIC         sum(actual) as tot_sales,
# MAGIC         sum(predict) as predicted_sales
# MAGIC     from sashelp.prdsale
# MAGIC     group by 1 order by month;
# MAGIC quit;

# COMMAND ----------

sales = sas.sasdata('sales')
sales.series(y=['tot_sales','predicted_sales'], x='month', title='total vs. predicted sales')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There is support for a [SASets](https://sassoftware.github.io/saspy/api.html#saspy.sasstat.SASstat).

# COMMAND ----------

#TODO: check why not rendering
ets = sas.sasets()
ets_results = ets.timeid(id='horsepower', data=cars)
dir(ets_results)


# COMMAND ----------

ets_results.ALL()

# COMMAND ----------

# MAGIC %md
# MAGIC You also have access to the SAS OS file system:

# COMMAND ----------

# get a list of files and directories for this directory. Directories end with the file seperator
# We can take a look at where we cloned this prject in SAS
sas.dirlist('/export/sas-viya/data/repos/sas_interop')

# COMMAND ----------

# MAGIC %md
# MAGIC You also have the ability to see what the SAS code for a given SASPy method will generate. This can be helpful if the process is better done in SAS code.

# COMMAND ----------

sas.teach_me_SAS(True)
sales.series(y=['tot_sales','predicted_sales'], x='month', title='total vs. predicted sales')

# COMMAND ----------

ets_results = ets.timeid(id='horsepower', data=cars)

# COMMAND ----------

#???
# stat_results = stat.reg(model='horsepower = Cylinders EngineSize', data=cars)

# COMMAND ----------

cars.describe()

# COMMAND ----------

cars.tail(24)

# COMMAND ----------

sas.teach_me_SAS(False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC When you no longer need a SASsession, you can close it by calling the `sas.endsas` method.

# COMMAND ----------

sas._endsas()
