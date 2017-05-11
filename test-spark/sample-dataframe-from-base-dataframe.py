
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

sc = SparkContext()
sqlContext = SQLContext(sc)

train = sqlContext.load(source = 'com.databricks.spark.csv', path = \
                        '/vagrant/test-spark/train.csv',
                        header = True, inferSchema = True)

test = sqlContext.load(source = 'com.databricks.spark.csv', path = \
                       '/vagrant/test-spark/test.csv',
                        header = True, inferSchema = True)

t1 = train.sample(False, 0.2, 42)
t2 = train.sample(False, 0.2, 43)
# print t1.count(),t2.count(), '\n'

# map operations on dataframe columns
# print train.select('User_ID').map(lambda x: (x, 1)).take(5), '\n'

# sort the dataframe based on columns (orderBy)
# print train.orderBy(train.Purchase.desc()).show(5), '\n'

# add new column in dataframe
# print train.withColumn('Purchase_new', train.Purchase / 2.0).select('Purchase', 'Purchase_new').show(5), '\n'

# drop a column in a dataframe
# print train.columns, '\n'
# print train.drop('Purchase').columns, '\n'

# remove some categories of Product_ID column in test that are not present in Product_ID column in train
diff_cat_in_train_test = test.select('Product_ID').subtract(train.select('Product_ID'))
# print diff_cat_in_train_test.distinct().count(), '\n'

not_found_cat = diff_cat_in_train_test.distinct().rdd.map(lambda x: x[0]).collect()

F1 = udf(lambda x: '-1' if x in not_found_cat else x, StringType())

k = test.withColumn('NEW_Product_ID', F1(test['Product_ID'])).select('NEW_Product_ID')

diff_cat_in_train_test=k.select('NEW_Product_ID').subtract(train.select('Product_ID'))
print diff_cat_in_train_test.distinct().count(), '\n'
