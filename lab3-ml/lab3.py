from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
h_distance = 300000
h_date = 15
h_time = 3
longitud = 58.4274 # Up to youl
latitud  = 14.826 # Up to you
date = "2013-07-04" # Up to you
stations = sc.textFile("BDA/input/stations.csv")
stations = stations.map(lambda line: line.split(";"))
temps = sc.textFile("BDA/input/temperature-readings.csv")
temps = temps.sample(False, 0.1)
temps  = temps.map(lambda line: line.split(";"))

# Your code here
# Filter Code
temps = temps.filter(lambda x: int(x[1][0:4]) <= int(date[0:4]))
temps = temps.filter(lambda x: int(x[1][5:7]) <= int(date[5:7]))
temps = temps.filter(lambda x: int(x[1][8:10]) <= int(date[8:10]))


# key,val - (station,date), (time,temp)
temps = temps.map(lambda x: ((x[0], x[1]), (x[2],x[3])))
# key, val - station, (longitud, latitud) GIVES map
stations_list = stations.map(lambda x: (x[0], (float(x[3]),float(x[4]))))
st_list = sc.broadcast(stations_list.collectAsMap())
# Broadcast gives key-value: (station, date, time), (temp, lo, la)
merged_temp = temps.map(lambda x: (  (x[0][0], x[0][1],  x[1][0]), (x[1][1], st_list.value.get(x[0][0]))   ))
st_list.unpersist()
merged_temp = merged_temp.cache()

#for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
#"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    # Your code here

last = merged_temp
last.saveAsTextFile("BDA/output")
