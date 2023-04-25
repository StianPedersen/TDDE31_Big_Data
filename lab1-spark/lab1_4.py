from pyspark import SparkContext
from operator import add

sc = SparkContext(appName = "exercise 1.4")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# Only necessary variables (stationnumber, temperature)
reduced = lines.map(lambda x: (x[0], float(x[3])))

# Filter out the tempratures outside 25-30

# Get the maximum tempratures for the stations
temperature_filtered = reduced.reduceByKey(lambda x,y: max(x,y))
temperature_filtered = temperature_filtered.filter(lambda x: float(x[1])>=25.0 and float(x[1])<=30.0)

# Precipation
precipation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipation_file.map(lambda line: line.split(";"))

# Only necessary variables (stationnumber,date, percipation)
precipation_reduced = lines.map(lambda x: ((x[0],x[1]), float(x[3])))

# Group by the stationnumber and date, add the percipation
grouped_precipation = precipation_reduced.groupByKey()
precipation_added = grouped_precipation.map(lambda x: (x[0][0],sum(x[1])))

# Get only stations with percipation between 100 -> 200

# Keep only the max
precipation_max = precipation_added.reduceByKey(lambda x,y: max(x,y))
precipation_filtered = precipation_max.filter(lambda x: float(x[1])>=100 and float(x[1])<=200)

# join precipation and temperature
last = temperature_filtered.join(precipation_filtered)
last = last.sortBy(ascending = False, keyfunc=lambda x: x[0])
last.saveAsTextFile("BDA/output")
