from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1")
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# Get years between 1950 and 2014
year_temperature = lines.filter(lambda x: int(x[1][0:4])>=1950 and int(x[1][0:4])<=2014)
# All tempratures above 10.0
above_10_temp = year_temperature.filter(lambda x: float(x[3]) > 10.0)

# Add all distinct tempratures that are above 10
# FOR non distinctr values
key_thing = above_10_temp.map(lambda x: (x[0], x[1][0:7]))

# For distinct values
# key_thing = above_10_temp.map(lambda x: (x[0], x[1][0:7])).distinct()

key_thing  = key_thing.map(lambda x: ((x[1][0:7]), 1)).reduceByKey(lambda x,y: x+y )
key_thing = key_thing.sortBy(ascending = False, keyfunc=lambda x: x[0])
key_thing.saveAsTextFile("BDA/output")
