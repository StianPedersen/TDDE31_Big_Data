from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Lab2")
sqlContext = SQLContext(sc)

#Loading text file and convert each line to a Row
precipation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines_p = precipation_file.map(lambda line: line.split(";"))
lines_o = ostergotland_file.map(lambda line: line.split(";"))

perc = lines_p.map(lambda p: Row(station=p[0], year=int(p[1].split("-")[0]),month = int(p[1].split("-")[1]), perc=float(p[3])))
ost_stations = lines_o.map(lambda p: Row(station=p[0]))
perc = sqlContext.createDataFrame(perc)
ost_stations = sqlContext.createDataFrame(ost_stations)

perc.registerTempTable("precipitation")
ost_stations.registerTempTable("ost_stations")

perc_filtered = perc.filter( (perc['year'] >= 1993) & (perc['year'] <= 2016))
perc_grouped = perc_filtered.groupBy(perc_filtered['year'],perc_filtered['month'],perc_filtered['station']).agg(F.sum(perc_filtered['perc']).alias('sum_rain'))
perc_grouped_ym = perc_grouped.join(ost_stations, ['station'],'inner').select('year','month','sum_rain')
# Should have all the correct stations
perc_grouped_ym = perc_grouped_ym.groupBy(perc_grouped_ym['year'],perc_grouped_ym['month']).agg(F.avg(perc_grouped_ym['sum_rain']).alias('avg_rain'))
lab2_5 = perc_grouped_ym.orderBy(perc_grouped_ym['year'],perc_grouped_ym['month'],ascending=False)
print("ABCDE")
lab2_5.show()
last = lab2_5.rdd
last.saveAsTextFile("BDA/output")
