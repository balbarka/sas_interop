# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS Programming Language and SAS Base
# MAGIC
# MAGIC SAS Programming Language and SAS Base are related but distinct components of the SAS (Statistical Analysis System) software suite.
# MAGIC  * **SAS Programming Language**:
# MAGIC    * The SAS Programming Language refers to the entire language and syntax used in SAS to write programs for data manipulation, statistical analysis, and reporting.
# MAGIC    * It encompasses the data step, PROC (Procedure) steps, and other programming constructs used for various tasks.
# MAGIC    * The SAS Programming Language provides a comprehensive set of data manipulation functions, data processing capabilities, and statistical procedures that form the foundation of SAS.
# MAGIC    * It includes various data step functions for data transformation and processing, as well as PROC steps for performing specific analytical tasks like statistical analysis, data summarization, and reporting.
# MAGIC
# MAGIC  * **SAS Base**:
# MAGIC    * SAS Base is a subset or core component of the SAS system that includes essential functionalities needed for data management and basic statistical analysis.
# MAGIC    * It comprises the basic data step, basic PROC steps, and the SAS Macro Language.
# MAGIC    * SAS Base is often considered the foundation of the SAS software suite, and it provides a comprehensive set of tools for data manipulation and analysis.
# MAGIC     * While SAS Base includes a significant portion of the SAS Programming Language, it doesn't include the more advanced statistical procedures and modules that are part of other SAS components like SAS/STAT, SAS/GRAPH, etc.
# MAGIC
# MAGIC **NOTE**: This notebook will focus on the coding concept differences between SAS and Python. Those base SAS components like data step, basic PROCs, and Macro Language will be covered in following notebooks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Python & SAS - Language Comparison
# MAGIC
# MAGIC Python is a high-level, general-purpose programming language that is:
# MAGIC   * **Interpreted** - Refers to a type of programming language where the source code is executed directly by an interpreter at runtime, without the need for prior compilation into machine code. The interpreter reads the source code line by line and translates it into machine code or intermediate code on the fly, executing the instructions one by one. For new users the largest impact is that there is no compile step between writing and executing code.
# MAGIC   * **Dynamically-typed** - In a dynamically-typed language, you can assign a value of any data type to a variable without explicitly specifying the data type. Further, within the runtime a varaible can be reassigned to a diffrent data type.
# MAGIC   * **object-oriented** - Object-oriented programming (OOP) is a programming paradigm or approach that organizes code and data into objects, which are self-contained units representing real-world entities, concepts, or abstractions. It is one of the most widely used programming paradigms and is based on the principles of:
# MAGIC     * **Encapsulation**: Encapsulation refers to the practice of bundling data and methods within a class, hiding the internal details from the outside world. It provides a clear interface for interacting with objects while protecting their internal implementation.
# MAGIC     * **Inheritance**: Inheritance allows one class (called the subclass or derived class) to inherit the attributes and methods of another class (called the superclass or base class). This enables code reuse and supports the creation of hierarchical relationships between classes.
# MAGIC     * **Polymorphism**: Polymorphism allows objects of different classes to be treated as objects of a common superclass. It enables a single interface to represent multiple types, facilitating code flexibility and extensibility.
# MAGIC     * **Abstraction**: Abstraction involves simplifying complex systems by focusing on the essential characteristics while hiding unnecessary details. In OOP, classes and objects provide abstraction by encapsulating data and behavior.
# MAGIC
# MAGIC SAS (Statistical Analysis System) is a domain-specific language (DSL) designed specifically for statistical analysis, data manipulation, and reporting. It's commitment to stay analytical programming has made it popular amoung analyts and contributed it's language characteristics:
# MAGIC   * **Interpreted** - SAS is primarily an interpreted language. When you write SAS programs, they are executed directly by the SAS interpreter without the need for a separate compilation step. The SAS interpreter reads and processes the SAS code line by line, executing each statement sequentially.
# MAGIC   * **Statically-typed** - This means that variables must be explicitly declared with their data types before they can be used in the program.
# MAGIC   * **Proceedural** - SAS is traditionally considered a procedural language with some declarative aspects, where programs are organized as a sequence of steps that manipulate data and perform statistical analyses. In SAS, you work with datasets and use procedures (PROCs) to perform specific tasks like data summarization, statistical analysis, and reporting. SAS programs are typically written in a procedural style rather than following the principles of object-oriented programming (there are concept exceptions like [ODS](https://documentation.sas.com/doc/en/pgmsascdc/v_037/statug/statug_odsgraph_sect025.htm) and [PROC FCMP](https://documentation.sas.com/doc/en/pgmsascdc/v_037/proc/n1aozmc89vjkpzn1q6a54nleh56o.htm)).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Impact of Interpreted Languages
# MAGIC
# MAGIC Since, both python and SAS are interpreted, we should expect the same REPL experience right?... Not really. SAS provides formatted output via output tab and conveniently shows logs as a separate tab. This is the expected user experience where the output from many procs may result in a single artifact that can be saved. This will differ from an ipython kernel running in a notebook. In notebook form, every segment of code in a cell will have a corresponding cell output that will keep successive iterations. Further, while there is logging for python, those logs are typically only engaged when debugging. The only log output you will see in a python script is when an exception is thrown - in that case, by default the cell is populated with the call trace to help the user identify the problem. Here are some examples of evaluating cells.
# MAGIC
# MAGIC **NOTE**: To run a cell and advance you can hit `shift+ENTER`. To run a cell and not advance you can hit `ctrl+ENTER`. Alternately, you can execute via mouse by clicking on the run (play) icon in the top right of the cell.

# COMMAND ----------

10 + 10

# COMMAND ----------

print("Hello World!!")

# COMMAND ----------

# Multi-line is python is """ or '''
# We can display html directly in the notebook with display html (we'll use this to render SAS html later)
displayHTML("""
<div style="font-size: 36px; color: red;">Hello World!!!</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Imports for analytic language tasks
# MAGIC
# MAGIC SAS and R have basic analytic tasks loaded and ready to go the first time start the session. However, python is general purpose and doesn't pre-load analytic methods by default. As a consequence, there are some very popular libraries within python that you will need to import. They each have their own common library shorthand as well:
# MAGIC
# MAGIC | Library | Shorthand | description |
# MAGIC | ------- | --------- | ----------- |
# MAGIC | [NumPy](https://numpy.org/doc/stable/user/quickstart.html) | `np` | Provides support for large, multi-dimensional arrays and matrices, along with a collection of high-level mathematical functions to operate on these arrays efficiently. (Think ) |
# MAGIC | [SciPy](http://scipy.github.io/devdocs/tutorial/index.html) | `sp` | Use the SciPy library in Python for its extensive collection of scientific and mathematical functions, including optimization, integration, interpolation, signal processing, linear algebra, and more, making it a powerful tool for various scientific and engineering applications. |
# MAGIC | [Pandas](https://pandas.pydata.org/docs/user_guide/10min.html) | `pd` | Use the pandas Python library for its powerful data manipulation and analysis capabilities, including data alignment, cleaning, transformation, filtering, and visualization, making it a valuable tool for working with structured data efficiently. The data manipulation is on a the pandas class `DataFrame`. | 
# MAGIC | [Matplotlib ](https://matplotlib.org/stable/gallery/index.html) | `plt` | Use Matplotlib for its extensive functionality and flexibility in creating high-quality, customizable plots and visualizations. It's a popular tool for data exploration, analysis, and communication in various domains. |
# MAGIC | [SciKit-Learn](https://scikit-learn.org/stable/) | `sk` | You would want to use scikit-learn in Python for its large collection of machine learning algorithms, user-friendly API, and robust tools for data preprocessing, model training, and evaluation, making it an essential library for building and deploying machine learning models with ease and efficiency. This library will be a part of most non-distributed machine learning applications. |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Numpy Example

# COMMAND ----------

# MAGIC %%SAS log
# MAGIC
# MAGIC /* Example SAS code for matrix multiplication using PROC IML */
# MAGIC proc iml;
# MAGIC /* Define two matrices */
# MAGIC A = {1 2,
# MAGIC      3 4};
# MAGIC
# MAGIC B = {5 6,
# MAGIC      7 8};
# MAGIC
# MAGIC /* Perform matrix multiplication */
# MAGIC matrix_result = matrix_A * matrix_B;
# MAGIC
# MAGIC /* Print the matrices and the result */
# MAGIC print matrix_A[r=Matrix_A c=Matrix_A];
# MAGIC print matrix_B[r=Matrix_B c=Matrix_B];
# MAGIC print matrix_result[r=Matrix_Result c=Matrix_Result];
# MAGIC quit;

# COMMAND ----------

import numpy as np

# Define two matrices as NumPy arrays
matrix_A = np.array([[1, 2], [3, 4]])
matrix_B = np.array([[5, 6], [7, 8]])

# Perform matrix multiplication using numpy.dot() function
result_dot = np.dot(matrix_A, matrix_B)
# Alternatively, you can use the @ operator for matrix multiplication in Python 3.5 and later:
# result_dot = matrix_A @ matrix_B

print("Matrix A:")
print(matrix_A)

print("\nMatrix B:")
print(matrix_B)

print("\nMatrix multiplication result:")
print(result_dot)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SciPy example

# COMMAND ----------

# MAGIC %%SAS lst
# MAGIC
# MAGIC proc nlp;
# MAGIC min f;
# MAGIC decvar x1 x2;
# MAGIC f1 = 10 * (x2 - x1 * x1);
# MAGIC f2 = 1 - x1;
# MAGIC f = .5 * (f1 * f1 + f2 * f2);
# MAGIC run;
# MAGIC

# COMMAND ----------

import numpy as np
from scipy.optimize import minimize

# Define the objective function to be minimized
def objective_function(x):
    x1, x2 = x
    f1 = 10 * (x2 - x1 * x1)
    f2 = 1 - x1
    f = 0.5 * (f1 * f1 + f2 * f2)
    return f

# Initial guess for the minimum (starting point of optimization)
initial_guess = [0, 0]

# Define the bounds for the variables
bounds = [(None, None), (None, None)]  # No bounds for x1 and x2

# Minimize the objective function using the "Nelder-Mead" method
result = minimize(objective_function, initial_guess, method='Nelder-Mead', bounds=bounds)

# Print the optimization result
print("Optimization Result:")
print("Minimum value found:", result.fun)
print("Optimal solution:", result.x)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Pandas Example
# MAGIC
# MAGIC **NOTE**: Pandas is so ubiquitous in Pythonic programming that many of the library's DataFrame conventions were adopted in the [Apache Spark](https://spark.apache.org/docs/latest/index.html) distributed computing framework. The python api for Spark is known as [pyspark](https://spark.apache.org/docs/latest/api/python/getting_started/index.html?highlight=quick%20start) which will be covered in later notebooks. The Pandas syntax for DataFrame manipulations is replicated in spark as the [pandas api](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html?highlight=pandas).
# MAGIC
# MAGIC **Note**: Unlike most objects in python, they are rendered as text when displayed. However, pandas dataframes are one where there is additional functionality built into the Databricks notebook that can be accessed in the output cell directly.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Create the SAS dataset with city names and populations */
# MAGIC data Cities;
# MAGIC     infile datalines delimiter='|';
# MAGIC     length City $20;
# MAGIC     input City $ Population;
# MAGIC     datalines;
# MAGIC New York|8537673
# MAGIC Los Angeles|3979576
# MAGIC Chicago|2693976
# MAGIC Houston|2131024
# MAGIC ;
# MAGIC run;
# MAGIC
# MAGIC /* Filter the dataset for cities with more than 3 million people */
# MAGIC data Cities_Filtered;
# MAGIC     set Cities;
# MAGIC     where Population > 3000000;
# MAGIC run;
# MAGIC
# MAGIC /* Display the filtered dataset */
# MAGIC proc print data=Cities_Filtered noobs;
# MAGIC run;

# COMMAND ----------

import pandas as pd

# Create the DataFrame of city names and their population
city_df = pd.DataFrame({'City': ['New York', 'Los Angeles', 'Chicago', 'Houston'],
                        'Population': [8537673, 3979576, 2693976, 2131024]})

# Filter the DataFrame for cities with more than 3 million people
large_city_df = city_df[city_df['Population'] > 3000000]

print(large_city_df)

# COMMAND ----------

display(city_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Matplotlib Example
# MAGIC
# MAGIC **NOTE**: It is helpful to browse the [library docs](https://matplotlib.org/stable/gallery/index.html) to get an idea of all the plot types and how argumented.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC /* Create the dataset with 100 x, y pairs */
# MAGIC data ScatterData;
# MAGIC     do i = 1 to 100;
# MAGIC         x = rand("Uniform") * 100; /* Generate random x between 0 and 100 */
# MAGIC         y = rand("Normal", 50, 10); /* Generate random y with mean 50 and standard deviation 10 */
# MAGIC         output;
# MAGIC     end;
# MAGIC run;
# MAGIC
# MAGIC /* Create the scatter plot */
# MAGIC proc sgplot data=ScatterData;
# MAGIC     title "Scatter Plot of 100 x, y Pairs";
# MAGIC     scatter x=x y=y / markerattrs=(symbol=circlefilled) name="scatterplot";
# MAGIC     xaxis label="X";
# MAGIC     yaxis label="Y";
# MAGIC run;

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 100
x = np.random.rand(100) * 100

# Generate 100 random y values with mean 50 and standard deviation 10
y = np.random.normal(50, 10, 100)

# Create the scatter plot
plt.figure()
plt.scatter(x, y, marker='o', c='blue', label='Data')
plt.xlabel('X')
plt.ylabel('Y')
plt.title('Scatter Plot of 100 x, y Pairs')
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SciKit-Learn
# MAGIC
# MAGIC In addition to tools for estimators, evaluation, and feature engineering, scikitlearn also has [pipelines](). Scikit-learn pipelines are a way to chain multiple data preprocessing steps and machine learning algorithms together in a single object, streamlining the process of data transformation and model building. [Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html) are also used in [pyspark ml](https://spark.apache.org/docs/2.3.0/api/python/pyspark.ml.html#pyspark-ml-package) which has based it's approach from scikit-learn.

# COMMAND ----------

# MAGIC %%SAS 
# MAGIC
# MAGIC /* Set a random seed for reproducibility (optional) */
# MAGIC data _null_;
# MAGIC     call streaminit(42);
# MAGIC run;
# MAGIC
# MAGIC /* Generate 100 random x values between 0 and 10 */
# MAGIC data Data;
# MAGIC     do i = 1 to 100;
# MAGIC         x = rand('Uniform') * 10 - 5;
# MAGIC         output;
# MAGIC     end;
# MAGIC run;
# MAGIC
# MAGIC /* Generate corresponding y values with some noise */
# MAGIC data Data;
# MAGIC     set Data;
# MAGIC     y = 4 * x**3 - x**2 + 5 * x + rand('Normal', 0, 30);
# MAGIC     x2 = x**2;
# MAGIC     x3 = x**3;
# MAGIC run;
# MAGIC
# MAGIC PROC SORT DATA = Data OUT = Data;
# MAGIC BY x y;
# MAGIC RUN;
# MAGIC /* Fit a polynomial regression model using PROC REG */
# MAGIC
# MAGIC proc reg data = Data;
# MAGIC     model y = x x2 x3 / noint;
# MAGIC     output out=FitPredicted predicted=y_pred;
# MAGIC run;
# MAGIC
# MAGIC
# MAGIC
# MAGIC /* Plot the predicted curve through the data points */
# MAGIC proc sgplot data=FitPredicted;
# MAGIC     scatter x=x y=y / markerattrs=(symbol=circlefilled) name="scatterplot";
# MAGIC     xaxis label="x";
# MAGIC     yaxis label="y";
# MAGIC     series x=x y=y_pred / lineattrs=(color=red) name="fitplot";
# MAGIC     refline 0 / axis=x lineattrs=(pattern=dash) name="refline";
# MAGIC     keylegend "scatterplot" "fitplot";
# MAGIC     title "Polynomial Regression Fit";
# MAGIC run;

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from scipy.stats import probplot
from matplotlib.pyplot import figure

figure(figsize=(16, 16), dpi=80)

# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 10
x = np.random.rand(100) * 10 - 5

# Generate corresponding y values with some noise
y = 4 * x**3 - x**2 + 5 * x + np.random.normal(0, 30, 100)

# Reshape the x array to a column vector (required for scikit-learn)
x = x.reshape(-1, 1)

# Create polynomial features up to degree 3
poly = PolynomialFeatures(degree=3)
x_poly = poly.fit_transform(x)

# Train a polynomial regression model
model = LinearRegression()
model.fit(x_poly, y)

# Predict y values using the trained model
y_pred = model.predict(x_poly)

# Create a scatter plot of the observed data points
plt.subplot(2, 2, 1)
plt.scatter(x, y, label='Observed Data', color='blue')

# Sort x values for smoother curve plotting
x_sorted = np.sort(x, axis=0)

# Predict y values for the sorted x values
x_sorted_poly = poly.transform(x_sorted)
y_sorted_pred = model.predict(x_sorted_poly)

# Plot the predicted curve through the data points
plt.plot(x_sorted, y_sorted_pred, label='Polynomial Regression', color='red')

plt.xlabel('x')
plt.ylabel('y')
plt.title('Polynomial Regression with scikit-learn')
plt.legend()
plt.grid(True)

# Create a residual plot
residuals = y - y_pred
plt.subplot(2, 2, 2)
plt.scatter(y_pred, residuals, label='Residuals', color='blue')
plt.xlim([np.min(y_pred), np.max(y_pred)])
plt.axhline(y=0, linestyle='--', color='gray')
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residual Plot')
plt.grid(True)

# Create a Cook's distance plot
plt.subplot(2, 2, 3)
influence = model.get_influence()
(c, _) = influence.cooks_distance

plt.stem(c, markerfmt=",")

plt.xlabel('Observation Index')
plt.ylabel("Cook's Distance")
plt.title("Cook's Distance Plot")
plt.grid(True)

# Create a Q-Q plot of the residuals
plt.subplot(2, 2, 4)
probplot(residuals, plot=plt)

plt.xlabel('Theoretical Quantiles')
plt.ylabel('Sample Quantiles')
plt.title('Q-Q Plot of Residuals')
plt.grid(True)

# Create a histogram of the residuals
plt.figure(figsize=(16, 8))
plt.hist(residuals, bins=20)

plt.xlabel('Residuals')
plt.ylabel('Frequency')
plt.title('Histogram of Residuals')
plt.grid(True)

plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from scipy.stats import probplot
from statsmodels.stats.outliers_influence import OLSInfluence
from matplotlib.pyplot import figure

figure(figsize=(16, 16), dpi=80)

# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 10
x = np.random.rand(100) * 10 - 5

# Generate corresponding y values with some noise
y = 4 * x**3 - x**2 + 5 * x + np.random.normal(0, 30, 100)

# Reshape the x array to a column vector (required for scikit-learn)
x = x.reshape(-1, 1)

# Create polynomial features up to degree 3
poly = PolynomialFeatures(degree=3)
x_poly = poly.fit_transform(x)

# Train a polynomial regression model
model = LinearRegression()
model.fit(x_poly, y)

# Predict y values using the trained model
y_pred = model.predict(x_poly)

# Create a scatter plot of the observed data points
plt.subplot(2, 2, 1)
plt.scatter(x, y, label='Observed Data', color='blue')

# Sort x values for smoother curve plotting
x_sorted = np.sort(x, axis=0)

# Predict y values for the sorted x values
x_sorted_poly = poly.transform(x_sorted)
y_sorted_pred = model.predict(x_sorted_poly)

# Plot the predicted curve through the data points
plt.plot(x_sorted, y_sorted_pred, label='Polynomial Regression', color='red')

plt.xlabel('x')
plt.ylabel('y')
plt.title('Polynomial Regression with scikit-learn')
plt.legend()
plt.grid(True)

# Create a residual plot
residuals = y - y_pred
plt.subplot(2, 2, 2)
plt.scatter(y_pred, residuals, label='Residuals', color='blue')
plt.xlim([np.min(y_pred), np.max(y_pred)])
plt.axhline(y=0, linestyle='--', color='gray')
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residual Plot')
plt.grid(True)

# Calculate Cook's distance
influence = OLSInfluence(model)
(c, _) = influence.cooks_distance

# Create a Cook's distance plot
plt.subplot(2, 2, 3)
plt.stem(c, markerfmt=",")

plt.xlabel('Observation Index')
plt.ylabel("Cook's Distance")
plt.title("Cook's Distance Plot")
plt.grid(True)

# Create a Q-Q plot of the residuals
plt.subplot(2, 2, 4)
probplot(residuals, plot=plt)

plt.xlabel('Theoretical Quantiles')
plt.ylabel('Sample Quantiles')
plt.title('Q-Q Plot of Residuals')
plt.grid(True)

# Create a histogram of the residuals
plt.figure(figsize=(16, 8))
plt.hist(residuals, bins=20)

plt.xlabel('Residuals')
plt.ylabel('Frequency')
plt.title('Histogram of Residuals')
plt.grid(True)

plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from matplotlib.pyplot import figure

# Set the figure size to 16x8
figure(figsize=(16, 8), dpi=80)

# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 10
x = np.random.rand(100) * 10 - 5

# Generate corresponding y values with some noise
y = 4 * x**3 - x**2 + 5 * x + np.random.normal(0, 30, 100)

# Reshape the x array to a column vector (required for scikit-learn)
x = x.reshape(-1, 1)

# Create polynomial features up to degree 3
poly = PolynomialFeatures(degree=3)
x_poly = poly.fit_transform(x)

# Train a polynomial regression model
model = LinearRegression()
model.fit(x_poly, y)

# Predict y values using the trained model
y_pred = model.predict(x_poly)

# Calculate residuals
residuals = y - y_pred

# Create a scatter plot of the observed data points
plt.subplot(1, 2, 1)
plt.scatter(x, y, label='Observed Data', color='blue')

# Sort x values for smoother curve plotting
x_sorted = np.sort(x, axis=0)

# Predict y values for the sorted x values
x_sorted_poly = poly.transform(x_sorted)
y_sorted_pred = model.predict(x_sorted_poly)

# Plot the predicted curve through the data points
plt.plot(x_sorted, y_sorted_pred, label='Polynomial Regression', color='red')

plt.xlabel('x')
plt.ylabel('y')
plt.title('Polynomial Regression with scikit-learn')
plt.legend()
plt.grid(True)

# Create a residual plot
plt.subplot(1, 2, 2)
plt.scatter(y_pred, residuals, label='Residuals', color='blue')
plt.axhline(y=0, linestyle='--', color='gray')
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residual Plot')
plt.grid(True)

# Show the plot with the new size
plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from matplotlib.pyplot import figure

figure(figsize=(16, 8), dpi=80)


# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 10
x = np.random.rand(100) * 10 - 5

# Generate corresponding y values with some noise
y = 4 * x**3 - x**2 + 5 * x + np.random.normal(0, 30, 100)

# Reshape the x array to a column vector (required for scikit-learn)
x = x.reshape(-1, 1)

# Create polynomial features up to degree 3
poly = PolynomialFeatures(degree=3)
x_poly = poly.fit_transform(x)

# Train a polynomial regression model
model = LinearRegression()
model.fit(x_poly, y)

# Predict y values using the trained model
y_pred = model.predict(x_poly)

# Create a scatter plot of the observed data points
plt.scatter(x, y, label='Observed Data', color='blue')

# Sort x values for smoother curve plotting
x_sorted = np.sort(x, axis=0)

# Predict y values for the sorted x values
x_sorted_poly = poly.transform(x_sorted)
y_sorted_pred = model.predict(x_sorted_poly)

# Plot the predicted curve through the data points
plt.plot(x_sorted, y_sorted_pred, label='Polynomial Regression', color='red')

plt.xlabel('x')
plt.ylabel('y')
plt.title('Polynomial Regression with scikit-learn')
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from scipy.stats import probplot
from statsmodels.stats.outliers_influence import OLSInfluence
from matplotlib.pyplot import figure

figure(figsize=(16, 16), dpi=80)

# Set a random seed for reproducibility (optional)
np.random.seed(42)

# Generate 100 random x values between 0 and 10
x = np.random.rand(100) * 10 - 5

# Generate corresponding y values with some noise
y = 4 * x**3 - x**2 + 5 * x + np.random.normal(0, 30, 100)

# Reshape the x array to a column vector (required for scikit-learn)
x = x.reshape(-1, 1)

# Create polynomial features up to degree 3
poly = PolynomialFeatures(degree=3)
x_poly = poly.fit_transform(x)

# Train a polynomial regression model
model = LinearRegression()
model.fit(x_poly, y)

# Predict y values using the trained model
y_pred = model.predict(x_poly)

# Create a scatter plot of the observed data points
plt.subplot(2, 2, 1)
plt.scatter(x, y, label='Observed Data', color='blue')

# Sort x values for smoother curve plotting
x_sorted = np.sort(x, axis=0)

# Predict y values for the sorted x values
x_sorted_poly = poly.transform(x_sorted)
y_sorted_pred = model.predict(x_sorted_poly)

# Plot the predicted curve through the data points
plt.plot(x_sorted, y_sorted_pred, label='Polynomial Regression', color='red')

plt.xlabel('x')
plt.ylabel('y')
plt.title('Polynomial Regression with scikit-learn')
plt.legend()
plt.grid(True)

# Create a residual plot
residuals = y - y_pred
plt.subplot(2, 2, 2)
plt.scatter(y_pred, residuals, label='Residuals', color='blue')
plt.xlim([np.min(y_pred), np.max(y_pred)])
plt.axhline(y=0, linestyle='--', color='gray')
plt.xlabel('Predicted Values')
plt.ylabel('Residuals')
plt.title('Residual Plot')
plt.grid(True)

# Calculate Cook's distance
influence = OLSInfluence(model)
(c, _) = influence.cooks_distance

# Create a Cook's distance plot
plt.subplot(2, 2, 3)
plt.stem(c, markerfmt=",")

plt.xlabel('Observation Index')
plt.ylabel("Cook's Distance")
plt.title("Cook's Distance Plot")
plt.grid(True)

# Create a Q-Q plot of the residuals
plt.subplot(2, 2, 4)
probplot(residuals, plot=plt)

plt.xlabel('Theoretical Quantiles')
plt.ylabel('Sample Quantiles')
plt.title('Q-Q Plot of Residuals')
plt.grid(True)

# Create a histogram of the residuals
plt.figure(figsize=(16, 8))
plt.hist(residuals, bins=20)

plt.xlabel('Residuals')
plt.ylabel('Frequency')
plt.title('Histogram of Residuals')
plt.grid(True)

plt.show()

# COMMAND ----------


