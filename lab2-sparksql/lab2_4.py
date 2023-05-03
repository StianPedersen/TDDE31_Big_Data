from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab2")
sqlContext = SQLContext(sc)

#Loading text file and convert each line to a Row
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0], date=p[1][0:7], year=int(p[1].split("-")[0]), time=p[2], temp=float(p[3]), quality=p[4]))
df_T = sqlContext.createDataFrame(tempReadings)
df_T.registerTempTable("tempReadings")

last = df_T_sorted.rdd
last.saveAsTextFile("BDA/output")
