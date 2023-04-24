from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1.5")
# This path is to the file on hdfs
precipation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_prec =  precipation_file.map(lambda line: line.split(";"))
lines_ost  = ostergotland_file.map(lambda line: line.split(";"))

# Filter 1993 -> 2016
lines_prec = lines_prec.filter(lambda x: int(x[1][0:4])>=1993 or int(x[1][0:4])<=2016)
# (station, YEAR-MONTH) (PREC)
lines_prec = lines_prec.map(lambda x: ((x[0], x[1][0:7]), float(x[3])))

# Collect gives a list
lines_ost = lines_ost.map(lambda x: (x[0])).collect()
station = sc.broadcast(lines_ost)
lines_prec = lines_prec.filter(lambda x: x[0][0] in station.value)

# Calculate avarage over lines_prec
added_prec = lines_prec.reduceByKey(lambda x,y: x+y)
# Year - Month - (Avarage,1)
avarage_prec = added_prec.map(lambda x: (x[0][1], (x[1], 1)))
avarage_prec = avarage_prec.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
avarage_prec = avarage_prec.map(lambda x: (x[0], x[1][0]/x[1][1]))
avarage_prec = avarage_prec.sortBy(ascending = False, keyfunc=lambda x: x[0])
last = avarage_prec
last.saveAsTextFile("BDA/output")

# # temperature
# temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# lines = temperature_file.map(lambda line: line.split(";"))
# reduced = lines.map(lambda x: (x[0], float(x[3])))
# temperature_filtered = reduced.filter(lambda x: float(x[1])>=25.0 and float(x[1])<=30.0)
# temperature_filtered = temperature_filtered.reduceByKey(lambda x,y: max(x,y))
#
#
# # Precipation
#
# lines = precipation_file.map(lambda line: line.split(";"))
# precipation_reduced = lines.map(lambda x: ((x[0],x[1]), float(x[3])))
# grouped_precipation = precipation_reduced.groupByKey()
# precipation_added = grouped_precipation.map(lambda x: (x[0][0],sum(x[1])))
# precipation_filtered = precipation_added.filter(lambda x: float(x[1])>=100 and float(x[1])<=200)
# precipation_max = precipation_filtered.reduceByKey(lambda x,y: max(x,y))
#
# #.reduceByKey(lambda a,b: a if a>=b else b)
