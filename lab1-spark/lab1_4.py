from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1.4")
# This path is to the file on hdfs

# temperature
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
reduced = lines.map(lambda x: (x[0], float(x[3])))
temperature_filtered = reduced.filter(lambda x: float(x[1])>=25.0 and float(x[1])<=30.0)
temperature_filtered = temperature_filtered.reduceByKey(lambda x,y: max(x,y))


# Precipation
precipation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipation_file.map(lambda line: line.split(";"))
precipation_reduced = lines.map(lambda x: ((x[0],x[1]), float(x[3])))
grouped_precipation = precipation_reduced.groupByKey()
precipation_added = grouped_precipation.map(lambda x: (x[0][0],sum(x[1])))
precipation_filtered = precipation_added.filter(lambda x: float(x[1])>=100 and float(x[1])<=200)
precipation_max = precipation_filtered.reduceByKey(lambda x,y: max(x,y))

#.reduceByKey(lambda a,b: a if a>=b else b)
# join precipation and temperature
last = temperature_filtered.join(precipation_max)
last.saveAsTextFile("BDA/output")
