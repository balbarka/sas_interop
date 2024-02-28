# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SASPy Setup
# MAGIC
# MAGIC ### Background:
# MAGIC [SASPy](https://github.com/sassoftware/saspy) is maintainted by SAS and provides an connection class to interface with an existing SAS deployment. Results returned can be text, HTML5 (the common output form in SAS), as well as pandas Dataframes. This interface is most useful when the predominance of operation is done in a non-SAS environement and either data or processes critical to the workflow / discovery are in SAS. Thus, one would most likely use SASpy from an ipython session, jupyter, or in our case Databricks notebook.
# MAGIC
# MAGIC <img src="https://github.com/balbarka/sas_interop/raw/main/ref/img/saspy_connection.png" alt="saspy_connection" width="600px">
# MAGIC
# MAGIC ### Refrences:
# MAGIC
# MAGIC   * [SASPy Python Package Documentation](https://sassoftware.github.io/saspy/)
# MAGIC   * [SASPy SAS Support](https://support.sas.com/en/software/saspy.html)
# MAGIC   * [SASPy SAS Example Notebook](https://github.com/sassoftware/saspy-examples/blob/main/SAS_contrib/saspy_example_github.ipynb) - We will base the databricks notebook example from this ipynb example.
# MAGIC   * [SASPy Releases](https://github.com/sassoftware/saspy/releases)
# MAGIC   * [SASPy PyPI](https://pypi.org/project/saspy/)
# MAGIC
# MAGIC
# MAGIC ### Setup Tasks:
# MAGIC   1. Installing SASPy library
# MAGIC   2. Creating a SASPy config
# MAGIC   3. Instantiate sas
# MAGIC   4. Create Magic, %SAS
# MAGIC   5. Create Magic, %SAS_file
# MAGIC   6. Automatic Setup via Init Scripts
# MAGIC   7. Managing Secrets
# MAGIC
# MAGIC ### Automatic Setup via Init Scripts
# MAGIC   Since we will likely not need to change our cluster connection config from cluster session to cluster session, we'll want to make it so that the configuration is completed for us automatically at setup.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Installing SASPy Library
# MAGIC
# MAGIC There are two locations that we can install SASPy from; Releases and PyPI. In example below, we will use [PyPI](https://pypi.org/project/saspy/).
# MAGIC
# MAGIC **NOTE**: The install location of the SASPy Library is important since this will be the default location for our config file. We will capture that location immediately after the install as `something`.

# COMMAND ----------

# MAGIC %pip install saspy

# COMMAND ----------

import saspy

# assign path variable to our eventual personal config
sascfg_personal_path = saspy.__file__.replace('__init__.py', 'sascfg_personal.py')
print(f'Eventual sascfg_personal.py path: \n{sascfg_personal_path}\n')

# List all configs found by SASPy
print(f'List of SASPy configs found: \n{saspy.list_configs()}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Create Personal SASPy Config
# MAGIC
# MAGIC Currently, there is a default config file provided with the SASPy package. However, it will not include the proper configurations for the SAS environment we will want to connect to. Thus, we will have to write our own personal configuration file. We will use the SASPy package default name, `sascfg_personal.py`, and the SASPy library install location for this.
# MAGIC
# MAGIC From [documentation](https://sassoftware.github.io/saspy/configuration.html) you can get the configuraitons you will need. This demo will be using the configurations for an [Azure SAS® Viya® (Pay-As-You-Go)](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/sas-institute-560503.sas-viya-on-azure) instance.
# MAGIC
# MAGIC Since we do not want to expose our credentials in this setup, we will be leveraging Databricks [secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/) to handle all our sensitive configuraitons. See bottom of notebook for detailed instructions.
# MAGIC
# MAGIC **NOTE**: Since the approach that we are using writes the connection configs into the cluster library install path, users will necessarily want to use a personal cluster for this configuration. If not, there will be collisions on users writing configuration files as well as configurations will be accessible to all users which introduces a security issues.
# MAGIC

# COMMAND ----------

sas_config_dict = {'url':  spark.conf.get("spark.sas_url"),
                   'user': spark.conf.get("spark.sas_user"),
                   'pw':   spark.conf.get("spark.sas_pwd"),
                   'context': 'SAS Job Execution compute context'}
sascfg_personal = \
f'''SAS_config_names   = ['hlssaspaygo']
hlssaspaygo = {str(sas_config_dict)}'''
with open(sascfg_personal_path,'w') as f:
    f.write(sascfg_personal)

with open(saspy.__file__.replace('__init__.py', 'sascfg_personal.py'),'r') as f:
    print(f.read())
    
# List all configs found by SASPy
print(f'\nList of SASPy configs found: \n{saspy.list_configs()}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Instantiate `sas`
# MAGIC
# MAGIC With the configuration out of the way, we are now able to instantiate our sas session. We'll pass two arguments into the `SASSession` class;
# MAGIC
# MAGIC  * `results` 'HTML', which will return the results as HTML which will provide a SAS usuer familiar output
# MAGIC  * `context` 'SAS Job Execution compute context'
# MAGIC  
# MAGIC  **NOTE**: There is an option to display results as `databricks`, however, this is intended for text display within a notebook which is usually not prefered over the HTML display.

# COMMAND ----------

sas = saspy.SASsession(results='HTML', context='SAS Job Execution compute context')

# COMMAND ----------

# We can confirm our connection settings:
sas

# COMMAND ----------

# We can run some of the example code to just verify that the connection works:
cars = sas.sasdata('cars', libref='sashelp')
cars.heatmap('msrp','horsepower')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Magic, %%SAS
# MAGIC
# MAGIC The most likely use of the SAS session for discovery will be iterative runs. This is most intuatively done in a notebook where the SAS output is displayed as a cell output and the input cell uses SAS native syntax. We can accomplish this using [python magics](https://ipython.readthedocs.io/en/stable/config/custommagics.html#defining-custom-magics).
# MAGIC
# MAGIC Most SAS users also take advantage of how SAS separates errors / logging from process output. In SAS Studio, this is conveniently done by providing tabs for both the logging in output. We will allow for comperable experience in notebooks, by allowing users to provide in the `%%SAS` magic command line the arguments `lst` or `out` with the following notebook displays:
# MAGIC
# MAGIC | lst as arg | log as arg | Display | 
# MAGIC |:----------:|:----------:| ------- |
# MAGIC | NO         | NO         | (Default) Displays Output as HTML |
# MAGIC | YES        | NO         | Displays Output as HTML |
# MAGIC | NO         | YES        | Displays LOGs as Text |
# MAGIC | YES        | YES        | Display both LOGs as Text and Output as HTML |

# COMMAND ----------

from IPython.core.magic import register_line_magic, register_cell_magic, register_line_cell_magic
from io import StringIO
from IPython.display import HTML 
@register_line_cell_magic
def SAS(line, cell):
    args = [a.lower() for a in line.split(' ')]
    show_LST = False if 'log' in args and 'lst' not in args else True
    show_LOG = True if 'log' in args else False
    c = sas.submit(cell)
    if show_LOG:
        print(c['LOG'])
    if show_LST:
        return HTML(c['LST'])
    else:
        return None

# COMMAND ----------

# MAGIC %%SAS log lst
# MAGIC proc print data=SASHELP.CARS (obs=10);
# MAGIC run;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Magic, %SAS_file
# MAGIC
# MAGIC In workflow notebooks that must call SAS, it may be more convenient to instead run a SAS file. This will avoid the complexity of having SAS text embedded within a notebook. This works by sending the entire file as a `sas.submit` command. For this next item, we will be running a file in this repo, [cars_display.sas](../sas/cars_display.sas).
# MAGIC
# MAGIC **NOTE**: Same as when using the `%%SAS` magic, you may pass the `lst`, `log` areguments to control display of the Output and log results in the notebook.

# COMMAND ----------

@register_line_magic
def SAS_file(line):
    args = [a.lower() for a in line.split(' ')]
    show_LST = False if 'log' in args and 'lst' not in args else True
    show_LOG = True if 'log' in args else False
    with open(args[0],'r') as f:
        c = sas.submit(f.read())
    if show_LOG:
        print(c['LOG'])
    if show_LST:
        return HTML(c['LST'])
    else:
        return None

# COMMAND ----------

# MAGIC %SAS_file ../sas/01-cars_display.sas lst log

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Automatic Setup via Init Scripts
# MAGIC
# MAGIC Thre are a couple files that we'll need to work with to make automatic setup work as desired:
# MAGIC  * <a href="$../.ipython/ipython_startup_init.sh" target="_blank">ipython_startup_init.sh</a> - This file will move ipython startup files into ipython startup path so that the databricks notebook ipython session runs the startup directory scripts
# MAGIC  * <a href="$../.ipython/profile_default/startup/01_sas.py" target="_blank">01_sas.py</a> - This is names as a profile startup file. It includes all logic necessary for installing and configuring SASPy. In addition, it will run the necessary configuration for ipython magics.
# MAGIC  
# MAGIC ### Setup:
# MAGIC  To get active the above automation, you will need to do the following:
# MAGIC  * Copy the .ipython directory that both folders are in to a shared location
# MAGIC  * Add environement variable IPYTHON_DIR to the cluster environement variable pointing to the share location
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Managing Secrets
# MAGIC
# MAGIC We want to be able to use sensitive keys without exposing them in notebooks or logs. Thus, we are going to use [Databricks Secrets](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets#--reference-a-secret-with-a-spark-configuration-property) with our secrets saved as environment variables. We shouldn't do this for shared clusters, but we will do it here since we are using personalized clusters.
# MAGIC
# MAGIC | scope | key | description |
# MAGIC | ----- | --- | ----------- |
# MAGIC | SAS   | url | The URL of the SAS Viya instance, in our case this is set to `https://hlssaspaygo.eastus2.cloudapp.azure.com/` |
# MAGIC | SAS   | user | The user you want to use for the SAS Session. |
# MAGIC | SAS   | pwd  | The password for the provided user, url. |
# MAGIC
# MAGIC Here is refrence code for how we set our secret variables:
# MAGIC
# MAGIC ``` bash
# MAGIC $> databricks secrets create-scope --scope-backend-type=DATABRICKS --scope=SAS
# MAGIC $> databricks secrets put --scope=SAS --key=url --string-value="https://XXXX.region.cloudapp.azure.com"
# MAGIC $> databricks secrets put --scope=SAS --key=user --string-value="XXXXX"                           
# MAGIC $> databricks secrets put --scope=SAS --key=pwd --string-value="XXXXXXXXXX"
# MAGIC ```
# MAGIC
# MAGIC You'll notice that we don't reference dbutils.secrets in <a href="$../.ipython/profile_default/startup/01_sas.py" target="_blank">01_sas.py</a> . Instead we use spark configs, ie. `spark.conf.get("spark.sas_url")`. To be able to access the secret as a config, we will [reference the secret as an environement variable](). Here are the configs that we put in our cluster definition to get this to work:
# MAGIC
# MAGIC **Spark config**:
# MAGIC ``` spark config
# MAGIC spark.sas_pwd {{secrets/SAS/pwd}}
# MAGIC spark.sas_user {{secrets/SAS/user}}
# MAGIC spark.sas_url {{secrets/SAS/url}}
# MAGIC ```

# COMMAND ----------

# Reference code to copy repo code to a shared location
dbutils.fs.cp('file:/Workspace/Repos/brad.barker@databricks.com/sas_interop/.ipython/profile_default/startup/01_sas.py',
              'dbfs:/FileStore/shared_uploads/brad.barker@databricks.com/.ipython/profile_default/startup/01_sas.py')
