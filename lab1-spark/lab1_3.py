from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1")
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
# Get values between 1960 and 2014
year_temperature = lines.filter(lambda x: int(x[1][0:4])>=1960 and int(x[1][0:4])<=2014)

# Map out the necessary values
key_thing = year_temperature.map(lambda x: ((x[0], x[1][0:7]), float(x[3])))

# Find min and max
max_key_thing = key_thing.reduceByKey(lambda x,y: max(x,y))
min_key_thing = key_thing.reduceByKey(lambda x,y: min(x,y))

# Join the min and max by keys
joined_min_max = max_key_thing.join(min_key_thing)

# Desired output format with avarage over the min/max
average = joined_min_max.map(lambda x: (x[0][1][0:4],x[0][1][5:7],x[0][0],float((float(x[1][0]) + float(x[1][1])) / 2)))
avarage = avarage.sortBy(ascending = False, keyfunc=lambda x: x[0] and x[1])
average.saveAsTextFile("BDA/output")
