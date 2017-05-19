from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

ut = sc.textFile('/vagrant/Uber.csv')

print 'No. of rows: ', ut.count(), '\n'
print 'Column header: ', ut.first(), '\n'

# distinct list count
rows = ut.map(lambda line: line.split(','))
distinct_rows_first_item = rows.map(lambda row: row[0]).distinct().count()
print distinct_rows_first_item
