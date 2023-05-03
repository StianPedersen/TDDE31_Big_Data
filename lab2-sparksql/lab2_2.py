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

#df_T_year = df_T.filter( (df_T['year'] >= 1950) & (df_T['year'] <= 2014) & (df_T['temp'] >= 10))
#Filter out all rows with 1950 <= temp <= 2014.
df_T_year = df_T.filter( ((df_T['year'] >= 1950) & (df_T['year'] <= 2014)))
#Filter out all rows with temp > 10.
df_T_year = df_T_year.filter(df_T['temp'] > 10.0)
#Drop all the rows where date and station is the same to get distinct.
df_T_year = df_T_year.dropDuplicates(["date", "station"]) #COMMENT out FOR NON DISTINCT.
#Count the number of rows.
df_T_with_count = df_T_year.groupBy(df_T_year['date']).count()
#Order by count
df_T_sorted = df_T_with_count.orderBy(df_T_with_count['count'],ascending=False)
df_T_sorted.show() #Is this one really needed? 

last = df_T_sorted.rdd
last.saveAsTextFile("BDA/output")
