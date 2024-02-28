# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS GRAPH
# MAGIC
# MAGIC Databricks provides basic [visualization](https://learn.microsoft.com/en-us/azure/databricks/visualizations/#------create-a-new-visualization) Python has a lot of high quality graphic libraries. Each of the links below hyperlink to their respective galleries. 
# MAGIC
# MAGIC | Library | Description |
# MAGIC | ------- | ----------- |
# MAGIC | [Matplotlib](https://matplotlib.org/stable/gallery/index.html) | Matplotlib is one of the most widely used and versatile plotting libraries in Python. It provides a wide range of plot types and customization options, making it suitable for various types of visualizations. |
# MAGIC | [Seaborn](https://seaborn.pydata.org/examples/index.html) | Seaborn is built on top of Matplotlib and offers higher-level functions for creating more visually appealing statistical visualizations. It simplifies the process of creating complex plots like heatmaps, pair plots, and violin plots. |
# MAGIC | [Plotly](https://plotly.com/python/) | Plotly is known for interactive and dynamic visualizations. It supports a variety of chart types and offers both Python and JavaScript APIs. It's particularly useful for creating web-based dashboards and interactive visualizations. |
# MAGIC | [Pandas Plotting](https://pandas.pydata.org/docs/user_guide/visualization.html) | The [Pandas](https://pandas.pydata.org/) library itself includes simple plotting functionality that allows you to create basic plots directly from Pandas DataFrames and Series. While not as feature-rich as other libraries, it's convenient for quick exploratory visualizations. |
# MAGIC | [Bokeh](https://docs.bokeh.org/en/latest/docs/gallery.html) |  Bokeh focuses on creating interactive, web-ready visualizations. It's designed for creating interactive plots that can be easily embedded into web applications and interactive dashboards. |
# MAGIC | [plotnine](https://plotnine.readthedocs.io/en/stable/gallery.html) (ggplot) | plotnine is based on the popular ggplot2 library in R and allows you to create complex visualizations using a grammar of graphics approach. Plotnine is the Python implementation of ggplot. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install bokeh
# MAGIC %pip install plotnine

# COMMAND ----------

bike_sdf = spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv", header="true", inferSchema="true")
bike_pdf = bike_sdf.toPandas()
display(bike_sdf)

# COMMAND ----------

# DBTITLE 1,Matplotlib
import matplotlib.pyplot as plt

# create two separate dataframes for working and non-working days
workingday_df = bike_pdf[bike_pdf['workingday'] == 1]
non_workingday_df = bike_pdf[bike_pdf['workingday'] == 0]

# create scatter plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.scatter(workingday_df['temp'], workingday_df['casual'], alpha=0.5, color='r', label='Working Day')
ax.scatter(non_workingday_df['temp'], non_workingday_df['casual'], alpha=0.5, color='b', label='Non-working Day')

# set the x-label, y-label, and title
ax.set_xlabel('Temperature (Normalized)')
ax.set_ylabel('Casual Bike Rentals')
ax.set_title('Scatter Plot of Temperature vs. Casual Bike Rentals')

# add legend
leg = ax.legend()

# show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1,Seaborn
import seaborn as sns
import matplotlib.pyplot as plt

# Create a scatter plot of temp and casual with dots colored by workingday
sns.set_style("darkgrid")
fig, ax = plt.subplots(figsize=(12, 6))
sns.scatterplot(x="temp", y="casual", hue="workingday", data=bike_pdf, ax=ax)

# Set the x-label and y-label
ax.set_xlabel('Temperature (Normalized)')
ax.set_ylabel('Casual Bike Rentals')

# Add a title to the plot
plt.title('Scatter Plot of Temperature vs. Casual Bike Rentals Colored by Working Day')

# Show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1,Plotly
import plotly.graph_objs as go
import plotly.offline as py

# Create a trace for workingday = 1
trace_wd1 = go.Scatter(
    x = bike_pdf[bike_pdf['workingday'] == 1]['temp'],
    y = bike_pdf[bike_pdf['workingday'] == 1]['casual'],
    mode = 'markers',
    name = 'Working Day'
)

# Create a trace for workingday = 0
trace_wd0 = go.Scatter(
    x = bike_pdf[bike_pdf['workingday'] == 0]['temp'],
    y = bike_pdf[bike_pdf['workingday'] == 0]['casual'],
    mode = 'markers',
    name = 'Non-working Day'
)

# Create the layout for the plot
layout = go.Layout(
    xaxis = dict(title = 'Temperature (Normalized)'),
    yaxis = dict(title = 'Casual Bike Rentals'),
    title = 'Scatter Plot of Temperature vs. Casual Bike Rentals Colored by Working Day',
    autosize = False,
    width = 1000, # increase width
    height = 600 # increase height
)

# Create the figure object
fig = go.Figure(data = [trace_wd1, trace_wd0], layout = layout)

# Display the plot
py.iplot(fig)

# COMMAND ----------

# DBTITLE 1,Pandas Plotting
import pandas as pd
import matplotlib.pyplot as plt

# Separate dataframes for working and non-working days
workingday_df = bike_pdf[bike_pdf['workingday'] == 1]
non_workingday_df = bike_pdf[bike_pdf['workingday'] == 0]

# Create a scatter plot with figsize = (12, 6)
fig, ax = plt.subplots(figsize=(12, 6))
workingday_df.plot(kind='scatter', x='temp', y='casual', color='r', label='Working Day', alpha=0.5, ax=ax)
non_workingday_df.plot(kind='scatter', x='temp', y='casual', color='b', label='Non-working Day', alpha=0.5, ax=ax)

# Set the x-label and y-label
plt.xlabel('Temperature (Normalized)')
plt.ylabel('Casual Bike Rentals')

# Add a title to the plot
plt.title('Scatter Plot of Temperature vs. Casual Bike Rentals Colored by Working Day')

# Add legend to the plot
plt.legend()

# Show the plot
plt.show()

# COMMAND ----------

# DBTITLE 1,Plotnine (R ggplot)
from plotnine import ggplot, aes, geom_point, scale_color_manual, labs, theme

# Create the plot
p = (ggplot(bike_pdf, aes(x='temp', y='casual', color='factor(workingday)')) +
     geom_point(size=4, alpha=0.7) +
     scale_color_manual(values=['red', 'blue']) +
     labs(x='Temperature (Normalized)', y='Casual Bike Rentals') +
     theme(legend_title_align='center', legend_position='top'))

# Show the plot
p.draw()
