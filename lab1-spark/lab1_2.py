from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

year_temperature = lines.filter(lambda x: int(x[1][0:4])>=1950 or int(x[1][0:4])<=2014)
above_10_temp = year_temperature.filter(lambda x: float(x[3]) > 10.0)
key_thing  = above_10_temp.map(lambda x: (x[1][0:7], 1) ).reduceByKey(lambda x,y: x+y)
# key_thing = key_thing.reduce(lambda x,y: x+y)


#key_thing.saveAsTextFile("BDA/output")

# part 1_2_b)
# ( (ID,DATE) , 1)
# ( DATE , 1)
                
key_thing = above_10_temp.map(lambda x: (x[0], x[1][0:7])).distinct()

key_thing  = key_thing.map(lambda x: ((x[1][0:7]), 1)).reduceByKey(lambda x,y: x+y )
key_thing.saveAsTextFile("BDA/output")

