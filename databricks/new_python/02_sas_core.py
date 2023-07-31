# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Data Steps
# MAGIC
# MAGIC There are too many datasteps to enumerate, but be assured that there is a method to accomplish the same in python. We'll take a look at only the most basic:
# MAGIC  - Join
# MAGIC  - Rename a Column
# MAGIC  - Drop a Column
# MAGIC  - Filtering
# MAGIC  - Aggregates
# MAGIC
# MAGIC While there are many data structure libraries for python. By far on of the most popular is [pandas]()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Joins

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Sample data for the left dataset */
# MAGIC data user;
# MAGIC     input uid name $ age;
# MAGIC     datalines;
# MAGIC 1 Alice 25
# MAGIC 2 Bob 30
# MAGIC 3 Charlie 22
# MAGIC 4 David 28
# MAGIC 5 Emma 35
# MAGIC ;
# MAGIC
# MAGIC /* Sample data for the right dataset */
# MAGIC data loc;
# MAGIC     input uid city $;
# MAGIC     datalines;
# MAGIC 1 Chicago
# MAGIC 2 Cincinnati
# MAGIC 3 Chicago
# MAGIC 4 Cincinnati
# MAGIC 5 Chicago
# MAGIC ;
# MAGIC
# MAGIC /* Perform the left join based on the 'uid' column */
# MAGIC data user_loc;
# MAGIC     merge user (in=a) loc (in=b);
# MAGIC     by uid;
# MAGIC     if a;
# MAGIC run;
# MAGIC
# MAGIC /* Display the merged dataset */
# MAGIC proc print data=user_loc;
# MAGIC run;

# COMMAND ----------

import pandas as pd

# Sample data for the left DataFrame
user_data = {
    'uid': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Emma'],
    'age': [25, 30, 22, 28, 35]
}
user = pd.DataFrame(user_data)

# Sample data for the right DataFrame
loc_data = {
    'uid': [1, 2, 3, 4, 5],
    'city': ['Chicago', 'Cincinnati', 'Chicago', 'Cincinnati', 'Chicago']
}
loc = pd.DataFrame(loc_data)

# Perform the left join based on the 'ID' column
user_loc = user.merge(loc, on='uid', how='left')

# Display the merged DataFrame
print(user_loc)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Rename a Column

# COMMAND ----------

# MAGIC %%SAS
# MAGIC data user_loc_rename;
# MAGIC     set user_loc(rename=(name=first_name));
# MAGIC run;
# MAGIC
# MAGIC /* Display the new dataset */
# MAGIC proc print data=user_loc_rename;
# MAGIC run;

# COMMAND ----------

# Python includes dict which allows you to map from a key to a value, this takes the form {<key>: <value>}
user_loc_rename = user_loc.rename(columns={'name': 'first_name'})
display(user_loc_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Drop a Column
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC data user_loc_drop(drop=age);
# MAGIC     set user_loc;
# MAGIC run;
# MAGIC
# MAGIC
# MAGIC /* Display the new dataset */
# MAGIC proc print data=user_loc_drop;
# MAGIC run;

# COMMAND ----------

# Note: pandas allows for higher dimensional tables and thus requires you to include the axis argument
user_loc_drop = user_loc.drop('age', axis=1)
display(user_loc_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filtering

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC data user_loc_filter;
# MAGIC     set user_loc;
# MAGIC     if age > 25;
# MAGIC run;
# MAGIC
# MAGIC /* Display the filtered dataset */
# MAGIC proc print data=user_loc_filter;
# MAGIC run;

# COMMAND ----------

# This functionality is similar to how R operates on dataframes - the interior filter is actually a boolean list
user_loc_filter = user_loc[user_loc.age > 25]
display(user_loc_filter)

# COMMAND ----------

print(user_loc.age > 25)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Aggregate
# MAGIC
# MAGIC

# COMMAND ----------

# Here we will find tha average age by city, sum is provided just to show multiple aggregates in the same group

agg_by_category = user_loc.groupby('city')['age'].agg(['sum', 'mean'])
print(agg_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # bamboolib
# MAGIC
# MAGIC However, there is also a tool for UI guided pandas transforms with bamboolib. This is helpful if you are new to using Pandas.
# MAGIC
# MAGIC To use bamboolib, just import bamboolib and run default display (write the dataframe variable with assignment or method)
# MAGIC import bamboolib
# MAGIC cake_df

# COMMAND ----------

import bamboolib
user_loc

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Keep rows where age < 25
user_loc_new = user_loc.loc[user_loc['age'] < 25]
display(user_loc_new)
