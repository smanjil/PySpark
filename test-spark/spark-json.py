# export PYTHONIOENCODING=utf-8 'in terminal'

from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

people = sqlContext.read.json("/vagrant/people.json")
print 'Schema: ', people.printSchema(), '\n'

people.registerTempTable("people")
print 'Records: ', sqlContext.sql("select * from people").show(), '\n'
