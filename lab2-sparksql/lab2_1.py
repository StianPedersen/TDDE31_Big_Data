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

schemaTempReadings.registerTempTable("tempReadings")
schemaTempReadings = schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014))

min = schemaTempReadings.groupBy(schemaTempReadings['year']).agg(F.min(schemaTempReadings['temp']).alias('temp'))
min_joined = min.join(schemaTempReadings,['year','temp'], 'inner').select('year','station','temp')
min_joined = min_joined.orderBy(min_joined['temp'],ascending=False)


max = schemaTempReadings.groupBy(schemaTempReadings['year']).agg(F.max(schemaTempReadings['temp']).alias('temp'))
max_joined = max.join(schemaTempReadings,['year','temp'], 'inner').select('year','station','temp')
max_joined = max_joined.orderBy(max_joined['temp'],ascending=False)
print("CODE ABCDE")
min_joined.show()
print("*****************************************************")
max_joined.show()



last = min_joined.rdd
last.saveAsTextFile("BDA/output")
