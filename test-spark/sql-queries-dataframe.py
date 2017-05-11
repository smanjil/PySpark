

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

train.registerTempTable('train_table')

# print sqlContext.sql('select Product_ID from train_table').show(5), '\n'

print sqlContext.sql('select Age, max(Purchase) as MaxPurchase from train_table group by Age').show(), '\n'
