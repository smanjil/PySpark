
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext()
sqlcontext = SQLContext(sc)

# creating dataframe from RDD
l = [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)]
rdd = sc.parallelize(l)
# print rdd.take(2)

people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
print 'People RDD: \n', people, '\n'

schemaPeople = sqlcontext.createDataFrame(people)
print 'Schemapeople collect(): \n', schemaPeople.collect(), '\n'
print 'Schemapeople show(): \n', schemaPeople.show(), '\n'

print 'Type of schemaPeople: \n', type(schemaPeople), '\n'
