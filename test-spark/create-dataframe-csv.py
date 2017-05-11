
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

train = sqlContext.load(source = 'com.databricks.spark.csv', path = \
                        '/vagrant/test-spark/train.csv',
                        header = True, inferSchema = True)

test = sqlContext.load(source = 'com.databricks.spark.csv', path = \
                       '/vagrant/test-spark/test.csv',
                        header = True, inferSchema = True)

# schema of train dataframe
# print train.printSchema(), '\n'

# see first n observations
# print train.head(2), '\n'

# see the records in row, cols format
# print train.show(2, truncate = True), '\n'

# count number of rows in a dataframe
# print train.count(), test.count(), '\n'

# count number of columns in a dataframe
# print len(train.columns), len(test.columns), '\n'

# get summary statistics of numerical columns of a dataframe
# print train.describe().show(), '\n'

# get the dscriptive statistics for a specific column
# print train.describe('Product_ID').show(), '\n'

# select columns from the dataframe
# print train.select('User_ID', 'Age').show(), '\n'

# number of distinct product in train and test files
# print train.select('Product_ID').distinct().count(), '\n'
# print test.select('Product_ID').distinct().count(), '\n'

# get the number of categories present in test but not in train
# diff_cat_test_train = test.select('Product_ID').subtract(train.select('Product_ID'))
# print diff_cat_test_train.distinct().count(), '\n'

# calculate pair wise frequency of categorical columns
# print train.crosstab('Age', 'Gender').show(), '\n'

# drop duplicates from dataframe
# print train.select('Age', 'Gender').dropDuplicates().show(), '\n'

# drop all rows with null values
# print train.dropna().count(), '\n'

# fill null values in a dataframe with a constant number
# print train.fillna(-1).show(2), '\n'

# filter the rows in train which has purchases more than 15000
# print train.filter(train.Purchase > 15000).count(), '\n'

# find the mean of purchase for each age group in the train set
# print train.groupby('Age').agg({'Purchase': 'mean'}).show(), '\n'

# get the count of each age groups
print train.groupby('Age').count().show(), '\n'
