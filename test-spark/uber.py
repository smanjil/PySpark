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
print 'Distinct first element of each row: ', distinct_rows_first_item, '\n'

# filter in rows
base02617 = rows.filter(lambda row: 'B02617' in row)
print 'B02617 in row: ', base02617.count(), '\n'
print 'Number of rows where base02617 had more than 15000 trips in a day: ', base02617.filter(lambda row: int(row[3]) > 15000).count(), '\n'

# reducebykey
filteredRows = ut.filter(lambda line: 'base' not in line).map(lambda line: line.split(','))
# print filteredRows.collect(), '\n'

print filteredRows.map(lambda kp: (kp[0], int(kp[3]))).reduceByKey(lambda k, v: k + v).takeOrdered(10, key=lambda x: -x[1]), '\n'
