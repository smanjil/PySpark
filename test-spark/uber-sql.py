from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/vagrant/Uber.csv')
# print df

# register temp table
df.registerTempTable('uber')

# finding distinct nyc based uber bases in the csv
distinct_bases = sqlContext.sql('select distinct dispatching_base_number from uber')
# for b in distinct_bases.collect(): print b

# see schema of df
print 'Schema: ', df.printSchema(), '\n'

# determining which Uber bases is the busiest based on number of trips
busiest_base = sqlContext.sql('''
    select distinct(dispatching_base_number), sum(trips) as cnt from uber group by
    dispatching_base_number order by cnt desc
''')
# print 'Busiest base: \n', busiest_base.show(), '\n'

# determining busiest day
busiest_day = sqlContext.sql('''
    select distinct(date), sum(trips) as cnt from uber group by date
    order by cnt desc limit 5
''')
print 'Busiest day: \n', busiest_day.show(), '\n'
