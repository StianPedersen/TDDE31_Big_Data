from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings-small.csv")
lines = temperature_file.map(lambda line: line.split(";"))

year_temperature = lines.filter(lambda x: int(x[1][0:4])>=1960 or int(x[1][0:4])<=2014)
key_thing = year_temperature.map(lambda x: ((x[0], x[1][0:7]), float(x[3])))
max_key_thing = key_thing.reduceByKey(lambda x,y: max(x,y))

# min_key_thing = key_thing.reduceByKey(lambda x,y: min(x,y))

max_key_thing.saveAsTextFile("BDA/output")

#above_10_temp = year_temperature.filter(lambda x: float(x[3]) > 10.0)
#key_thing  = above_10_temp.map(lambda x: (x[1][0:7], 1) ).reduceByKey(lambda x,y: x+y)

#key_thing = above_10_temp.map(lambda x: (x[0], x[1][0:7])).distinct()
#key_thing  = key_thing.map(lambda x: ((x[1][0:7]), 1)).reduceByKey(lambda x,y: x+y )
#key_thing.saveAsTextFile("BDA/output")

