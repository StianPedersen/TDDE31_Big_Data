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

#Get only the maximum temperatures.
df_max = df_T.groupBy(df_T['station']).agg(F.max(df_T['temp']).alias('temp'))
#Keep only the temp between 25-30.
df_25_30 = df_max.filter((df_max['temp'] >= 25.0) & (df_max['temp'] <= 30.0))


# Precipation
precipation_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipation_file.map(lambda line: line.split(";"))
precReadings = lines.map(lambda p: Row(station=p[0], date=p[1], perc=float(p[3])))
df_P = sqlContext.createDataFrame(precReadings)
df_P.registerTempTable("precReadings")

#Sum all the precipitation for each station each day
df_sum_prec = df_P.groupBy(['station', 'date']).agg(F.sum(df_P['perc']).alias('perc'))
df_max_prec = df_sum_prec.groupBy(['station', 'date']).agg(F.max(df_sum_prec['perc']).alias('max_perc'))
df_100_200 = df_max_prec.filter((df_max_prec['max_perc'] >= 100.0) & (df_max_prec['max_perc'] <= 200.0))

#Keep only the maximum precipitations for each station
# JOIN EM
joined = df_100_200.join(df_25_30['station'],'inner').select('station','temp','max_perc')


last = joined.rdd
last.saveAsTextFile("BDA/output")
