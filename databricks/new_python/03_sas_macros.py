# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # SAS Macros
# MAGIC
# MAGIC
# MAGIC In Python, the equivalent functionality for [SAS Macros](https://documentation.sas.com/doc/en/pgmsascdc/v_037/mcrolref/p1nypovnwon4uyn159rst8pgzqrl.htm) can be achieved using [python functions](https://docs.python.org/3/tutorial/controlflow.html#defining-functions) and [python classes](https://docs.python.org/3/tutorial/classes.html). While Python does not have a direct equivalent to SAS Macros, you can use functions and classes to perform macro tasks and achieve code reusability.
# MAGIC
# MAGIC For use to evaluate macro capability in python we'll look at writing a function that will take the sum of the first `x` elements of the Fibonacci series. First, let's look at SAS:
# MAGIC

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC %macro fibonacci(n);
# MAGIC     %if &n <= 0 %then 0;
# MAGIC     %else %if &n = 1 %then 1;
# MAGIC     %else %eval(%fibonacci(%eval(&n - 1)) + %fibonacci(%eval(&n - 2)));
# MAGIC %mend fibonacci;
# MAGIC
# MAGIC %macro fibonacci_series(x);
# MAGIC     %if &x <= 0 %then %do;
# MAGIC         %put ERROR: 'x' must be a positive integer greater than zero.;
# MAGIC         %return;
# MAGIC     %end;
# MAGIC
# MAGIC     %put Fibonacci Series with &x elements:;
# MAGIC     
# MAGIC     %local i;
# MAGIC     %do i = 0 %to &x -1;
# MAGIC         %put %eval(%fibonacci(&i));
# MAGIC     %end;
# MAGIC
# MAGIC %mend fibonacci_series;
# MAGIC
# MAGIC %macro fibonacci_series_sum(x);
# MAGIC     %if &x <= 0 %then %do;
# MAGIC         %put ERROR: 'x' must be a positive integer greater than zero.;
# MAGIC         %return;
# MAGIC     %end;
# MAGIC
# MAGIC     %let total_sum = 0; /* Initialize the cumulative sum */
# MAGIC
# MAGIC     %local i;
# MAGIC     %do i = 0 %to &x -1;
# MAGIC         %let total_sum = %eval(&total_sum + %fibonacci(&i));
# MAGIC     %end;
# MAGIC
# MAGIC     %put The sum of the Fibonacci Series with &x elements is &total_sum elements.;
# MAGIC
# MAGIC %mend fibonacci_series_sum;

# COMMAND ----------

# MAGIC %%SAS log
# MAGIC
# MAGIC %fibonacci_series(10);

# COMMAND ----------

# MAGIC %%SAS log
# MAGIC
# MAGIC %fibonacci_series_sum(10);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We've successfully written three macros in SAS using the loop functionality that come in the SAS macro language. We will write the same functions in python:
# MAGIC
# MAGIC | function      | decription |
# MAGIC | ------------- | ---------- |
# MAGIC | fibonacci(n)  | return the n-th value of the fibonacci series. |
# MAGIC | fibonacci_series(x) | %put or print the fibonacci series for the first x elements. | 
# MAGIC | fibonacci_series_sum(x) | %put or print the sum of the first x elements in the fibonacci series. |

# COMMAND ----------

def fibonacci(n):
    if n <= 0:
        raise ValueError("ERROR: 'n' must be a positive integer greater than zero.")
    elif n == 1:
        return 0
    elif n == 2:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)

def fibonacci_series(x):
    if x <= 0:
        raise ValueError("ERROR: 'x' must be a positive integer greater than zero.")
    print(f"Fibonacci Series with {x} elements:")
    for i in range(1, x + 1):
        print(fibonacci(i))

def fibonacci_series_sum(x):
    if x <= 0:
        raise ValueError("ERROR: 'x' must be a positive integer greater than zero.")
    total_sum = 0; # Initialize the cumulative sum
    for i in range(1, x + 1):
        total_sum = total_sum + fibonacci(i)
    print(f"The sum of the Fibonacci Series with {x} elements is {total_sum} elements.")



# COMMAND ----------

fibonacci_series(10)

# COMMAND ----------

fibonacci_series_sum(10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Error Handling
# MAGIC
# MAGIC SAS does have [SYSERR](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/mcrolref/n1wrevo4roqsnxn1fbd9yezxvv9k.htm) which can be used for exception handling. However, we didn't use it in our SAS code and it is not uncommon to be handled by writing to log/output in SAS.
# MAGIC
# MAGIC In the python example, we explicitly raised an exception using [ValueError](https://docs.python.org/3/library/exceptions.html?highlight=valueerror#ValueError). This is a programming feature that enables developers to raise an exception to the parent process. This is a critical component to general purpose programming and is helpful in stack tracking. More in depth examples include a try/capture where code that is susceptible to common failures can potentially be resolved. Error handling and logging are not new users topics, but be aware of this language feature if you come accross **\*Error**, **try**, or **except** in the wild.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Python Classes
# MAGIC
# MAGIC SAS is not primarily an object-oriented programming (OOP) language. While SAS has some object-oriented features, it is not designed as a full-fledged object-oriented language like Python, Java, or C++. Thus class concepts are not employed as much in SAS and likely SAS users already know how to accomplish tasks without using the concept of classes.
# MAGIC
# MAGIC Python classes allow you to define objects with attributes (variables) and methods (functions). Classes are useful for creating reusable code that can be instantiated multiple times with different data. Once you define a class, you can instantiate it many times and each instance will have all attributes and features that were declared in our class definition. To demonstrate the concept, I will create a class that is instantiated with a number of our choosing and can return our Fibonacci functions.

# COMMAND ----------

# NOTE: This class takes advantage of the fact that the function fibonacci is already defined above.

class Fibo():

    def __init__(self, n):
        if n <= 0:
            raise ValueError("ERROR: 'x' must be a positive integer greater than zero.")
        else:
            self.n = n

    def fibonacci_series(self):
        print(f"Fibonacci Series with {self.n} elements:")
        for i in range(1, self.n + 1):
            print(fibonacci(i))

    def fibonacci_series_sum(self):
        total_sum = 0; # Initialize the cumulative sum
        for i in range(1, self.n + 1):
            total_sum = total_sum + fibonacci(i)
        print(f"The sum of the Fibonacci Series with {self.n} elements is {total_sum} elements.")

# COMMAND ----------

# We can instantiate with any number we like... say 10
fibo = Fibo(10)

# COMMAND ----------

# Let's check our fibo method fibonacci_series. Notice how we didn't have to provide n since it was given at instantiation?
fibo.fibonacci_series()

# COMMAND ----------

# Let's check our fibo method fibonacci_series_sum. Notice how we didn't have to provide n since it was given at instantiation?
fibo.fibonacci_series_sum()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### List Comprehension
# MAGIC
# MAGIC You will need to be aware of one of the more common pythonic programming patterns, list comprehensions. These are syntactic sugar for writing Iterator logic succinctly. Effectively, using the key words **for** and **in** inside iterable brackets, we can write, lists, tuples, and even dicts. We'll provide a couple examples of the long form and short form:

# COMMAND ----------

# Let's create a list of number 0 to 6
numlist_long = []
for i in range(7):
    numlist_long.append(i)

numlist = [i for i in range(7)]

if numlist_long == numlist:
    print("numlist_long and numlist are exactly the same!")
    print(numlist)

# COMMAND ----------

# Let's import a library that has a method to get the name of the day of week from an integer
from calendar import day_name
days_of_week_long = []
for d in range(7):
    days_of_week_long.append(day_name[d])

days_of_week = [day_name[d] for d in range(7)]
if days_of_week_long == days_of_week:
    print("days_of_week_long and days_of_week are exactly the same!")
    print(days_of_week)

# COMMAND ----------

# We don't like that day_name maps 0 to monday. Let's make our own dict which makes sunday the first day of the week and the number starts at 1 instead of zero
# NOTE: in python % is also the modulo operator
new_day_name = {(d+1)%7:day_name[d] for d in range(7)}
new_day_name[0]

# COMMAND ----------

# You can inspect an individual item value if is has a default display by calling it 
new_day_name
