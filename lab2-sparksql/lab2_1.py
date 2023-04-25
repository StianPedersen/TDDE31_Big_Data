from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab2")
sqlContext = SQLContext(sc)

#Loading text file and convert each line to a Row
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0], date=p[1], year=int(p[1].split("-")[0]), time=p[2], temp=float(p[3]), quality=p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
#df =
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014))
min = schemaTempReadings.groupBy(schemaTempReadings['year'], schemaTempReadings['station']).agg(F.min(schemaTempReadings['temp']).alias('yearlymin'), F.max(schemaTempReadings['temp']).alias('yearlymax'))
# max = schemaTempReadings.groupBy(schemaTempReadings['year'], schemaTempReadings['station']).agg(F.max(schemaTempReadings['temp']))

#min_max_join = min.join(max, min['year'] == max['year'], 'inner')

#min_max_join = schemaTempReadings.groupBy('year', 'month', 'day','station').agg(F.min('value').alias('dailymin')).orderBy(['year', 'month', 'day', 'station'],ascending=[0,0,0,1])

last = min.rdd
last.saveAsTextFile("BDA/output")
