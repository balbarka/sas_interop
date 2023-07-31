# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SAS/STAT
# MAGIC
# MAGIC SAS/STAT is technically is a component of SAS Core which includes PROCs for statistical procedures and tools for data analysis and reporting. In this section, we will look at how some common SAS/STAT PROC would be written in python.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### PROC Means
# MAGIC
# MAGIC PROC Means can do a lot of things, we'll look at some common uses:
# MAGIC  * Get Description of columns
# MAGIC  * View Missing Values
# MAGIC  * t-test (we'll actually instead run a single sample ttest)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC options nodate pageno=1 linesize=80 pagesize=60;
# MAGIC data cake;
# MAGIC    input LastName $ 1-12 Age 13-14 PresentScore 16-17
# MAGIC          TasteScore 19-20 Flavor $ 23-32 Layers 34 ;
# MAGIC    datalines;
# MAGIC Orlando     27 93 80  Vanilla    1
# MAGIC Ramey       32 84 72  Rum        2
# MAGIC Goldston    46 68 75  Vanilla    1
# MAGIC Roe         38 79 73  Vanilla    2
# MAGIC Larsen      23 77 84  Chocolate  .
# MAGIC Davis       51 86 91  Spice      3
# MAGIC Strickland  19 82 79  Chocolate  1
# MAGIC Nguyen      57 77 84  Vanilla    .
# MAGIC Hildenbrand 33 81 83  Chocolate  1
# MAGIC Byron       62 72 87  Vanilla    2
# MAGIC Sanders     26 56 79  Chocolate  1
# MAGIC Jaeger      43 66 74             1
# MAGIC Davis       28 69 75  Chocolate  2
# MAGIC Conrad      69 85 94  Vanilla    1
# MAGIC Walters     55 67 72  Chocolate  2
# MAGIC Rossburger  28 78 81  Spice      2
# MAGIC Matthew     42 81 92  Chocolate  2
# MAGIC Becker      36 62 83  Spice      2
# MAGIC Anderson    27 87 85  Chocolate  1
# MAGIC Merritt     62 73 84  Chocolate  1
# MAGIC ;
# MAGIC proc means data=cake n mean max min range std fw=8;
# MAGIC    var PresentScore TasteScore;
# MAGIC    title 'Summary of Presentation and Taste Scores';
# MAGIC run;

# COMMAND ----------

cake_lst = [("Orlando", 27, 93, 80, "Vanilla", 1),
("Ramey", 32, 84, 72, "Rum", 2),
("Goldston", 46, 68, 75, "Vanilla", 1),
("Roe", 38, 79, 73, "Vanilla", 2),
("Larsen", 23, 77, 84, "Chocolate", None),
("Davis", 51, 86, 91, "Spice", 3),
("Strickland", 19, 82, 79, "Chocolate", 1),
("Nguyen", 57, 77, 84, "Vanilla", None),
("Hildenbrand", 33, 81, 83, "Chocolate", 1),
("Byron", 62, 72, 87, "Vanilla", 2),
("Sanders", 26, 56, 79, "Chocolate", 1),
("Jaeger", 43, 66, 74, None, 1), 
("Davis", 28, 69, 75, "Chocolate", 2),
("Conrad", 69, 85, 94, "Vanilla", 1),
("Walters", 55, 67, 72, "Chocolate", 2),
("Rossburger", 28, 78, 81, "Spice", 2),
("Matthew", 42, 81, 92, "Chocolate", 2),
("Becker", 36, 62, 83, "Spice", 2),
("Anderson", 27, 87, 85, "Chocolate", 1),
("Merritt", 62, 73, 84, "Chocolate", 1)]

cols = ["LastName", "Age", "PresentScore", "TasteScore", "Flavor", "Layers"] ;


import pandas as pd
cake_df = pd.DataFrame(cake_lst, columns=cols)
cake_mean_df = cake_df.describe()
cake_mean_df.reset_index(inplace=True)
cake_mean_df.rename(columns={'index': 'stat'})
display(cake_mean_df)

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC proc means data=cake N NMISS NOLABELS;
# MAGIC    Var PresentScore TasteScore Layers;
# MAGIC run;

# COMMAND ----------

# find the number of missing values in each column
null_counts = cake_df[['PresentScore','TasteScore','Layers']].isnull().sum()
print(null_counts)

# COMMAND ----------

# MAGIC %%SAS 
# MAGIC
# MAGIC proc ttest data=cake sides=2 h0=80;
# MAGIC       var TasteScore;
# MAGIC    run;

# COMMAND ----------

# import packages
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm, probplot
from scipy import stats

# The docs: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ttest_1samp.html#scipy-stats-ttest-1samp

# perform one sample t-test
t_statistic, p_value = stats.ttest_1samp(a=list(cake_df['TasteScore']), popmean=80)
print(f't Value: {t_statistic}')
print(f'Pr > |t|: {p_value}')


# Create a histogram of the data
plt.hist(cake_df['TasteScore'], bins=5, density=True, alpha=0.6, color='blue', label='Histogram')

# Fit a normal distribution to the data
mu, std = norm.fit(cake_df['TasteScore'])
x = np.linspace(60, 105, 100)
pdf = norm.pdf(x, mu, std)
plt.plot(x, pdf, 'r', linewidth=2, label='Fitted Normal Distribution')

# Set plot labels and legend
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.title('Histogram with Fitted Normal Distribution')
plt.legend()

# Show the histogram plot
plt.show()

# Create a Q-Q plot
probplot(cake_df['TasteScore'], plot=plt)

# Set plot labels and title
plt.xlabel('Theoretical Quantiles')
plt.ylabel('Sample Quantiles')
plt.title('Q-Q Plot')

# Show the plot
plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # PROC Rank
# MAGIC
# MAGIC PROC Rank has a lot of uses, we'll look at two:
# MAGIC  * Rank One Variable
# MAGIC  * Rank One Variable into Percentiles
# MAGIC
# MAGIC For both, we'll be operating on TasteScore Column
# MAGIC
# MAGIC A more common python approach is to create a new column on the source data for each analytic function. To do this we'll be assigning to a new column is a assignment that is very similar to R.
# MAGIC
# MAGIC **NOTE**: These analytic functions are also available in SQL, but we'll stick to what would be the python equivalent of a SAS transform.

# COMMAND ----------

# MAGIC %%SAS
# MAGIC
# MAGIC proc rank data=cake out=cake;
# MAGIC    var TasteScore;
# MAGIC    ranks TasteScore_rank;
# MAGIC run;
# MAGIC
# MAGIC proc rank data=cake groups=4 out=cake;
# MAGIC    var TasteScore;
# MAGIC    ranks TasteScore_qrt;
# MAGIC run;
# MAGIC
# MAGIC proc print data=cake;
# MAGIC run;

# COMMAND ----------

cake_df['TasteScore_rank'] = cake_df['TasteScore'].rank(method="average")
cake_df['TasteScore_qrt'] = pd.cut(cake_df['TasteScore'], 4, labels=range(4))

display(cake_df)
