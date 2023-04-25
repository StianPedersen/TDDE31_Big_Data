from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1")
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
# Get values between 1960 and 2014
year_temperature = lines.filter(lambda x: int(x[1][0:4])>=1960 and int(x[1][0:4])<=2014)

# Map out the necessary values
key_thing = year_temperature.map(lambda x: ((x[0], x[1]), float(x[3])))

# Find min and max
max_key_thing = key_thing.reduceByKey(lambda x,y: max(x,y))
min_key_thing = key_thing.reduceByKey(lambda x,y: min(x,y))

joined_min_max = max_key_thing.join(min_key_thing)
#(YYYY-MM, Station, temp, 1)
avarage_day = joined_min_max.map(lambda x: ((x[0][1][0:7], x[0][0]),(float((float(x[1][0]) + float(x[1][1])) / 2), 1)))
#Extract number of days
#Output -> ((u'1964-12', u'87150'), (1.3999999999999977, 31))
avarage_month = avarage_day.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avarage_month = avarage_month.map(lambda x: (x[0], (x[1][0]/x[1][1])))

# Desired output format with avarage over the min/max
#average = joined_min_max.map(lambda x: ((x[0][0],x[0][1][0:7]), x[1))
#average = average.map(lambda x: (x[0][1], x[0][0],float((float(x[1][0]) + float(x[1][1])) / 2)))
avarage_month = avarage_month.sortBy(ascending = False, keyfunc=lambda x: x[0])
avarage_month.saveAsTextFile("BDA/output")
