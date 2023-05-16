from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint, LassoWithSGD
from pyspark.mllib.classification import SVMWithSGD
from datetime import datetime
import numpy as np

sc = SparkContext(appName="lab_kernel")

stations = sc.textFile("BDA/input/stations.csv")
stations = stations.map(lambda line: line.split(";"))
temps = sc.textFile("BDA/input/temperature-readings.csv")
#temps = temps.sample(False, 0.1)
temps  = temps.map(lambda line: line.split(";"))


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
def distanceDiff(longitud1, latitud1, longitud2, latitud2):
  distance = haversine(longitud1, latitud1,longitud2,latitud2)
  return distance

def dateDiff(date1, date2):
    date1 = datetime(int(float(date1[0:4])),int(float(date1[5:7])),int(float(date1[8:10])))
    date2 = datetime(int(float(date2[0:4])),int(float(date2[5:7])),int(float(date2[8:10])))
    diff = date1 - date2
    days = diff.days
    days = days % 365
    if(days < 182.5):
        days = days
    else:
        days = 365 - days
    return days

def timeDiff(time1, time2):
  difference = abs(int(time1[0:2])-int(time2[0:2]))
  return difference
# Your code here
# Filter Code
date = "2013-07-04" # Up to you
longitud = 58.4274 # Up to youl
latitud  = 14.826 # Up to you
temps = temps.filter(lambda x: int(x[1][0:4]) <= int(date[0:4]))
temps = temps.filter(lambda x: int(x[1][5:7]) <= int(date[5:7]))
temps = temps.filter(lambda x: int(x[1][8:10]) <= int(date[8:10]))
print("START")

# key,val - (station,date), (time,temp)
temps = temps.map(lambda x: ((x[0], x[1]), (x[2],x[3])))
# temps = temps.sample(False, 0.1)
# key, val - station, (longitud, latitud) GIVES map
stations_list = stations.map(lambda x: (x[0], (float(x[3]),float(x[4]))))
st_list = sc.broadcast(stations_list.collectAsMap())
# Broadcast gives key-value: (station, date, time), (temp, lo, la)
merged_temp = temps.map(lambda x: (  (x[0][0], x[0][1],  x[1][0]), (x[1][1], st_list.value.get(x[0][0]))   ))
merged_temp_listed = merged_temp.map(lambda x: (x[1][0], x[0][0], x[0][1], x[0][2], x[1][1][0],x[1][1][1]))
# -> (u'-18.1', u'112080', u'1995-02-03', u'06:00:00', 61.1269, 12.8226)

# Map: tmp, year, month, day, time, lo, la         IS THIS CORRECT
calculated_list = merged_temp_listed.map(lambda x: (float(x[0]), int(x[2][0:4]), int(x[2][5:7]), int(x[2][8:10]) ,int(x[3][0:2]), x[4], x[5]))
calculated_list.cache()

def parsePoint(values):
    return LabeledPoint(values[0], values[1:])
featval = calculated_list.map(parsePoint)

# Model 1
model = LassoWithSGD.train(featval, iterations=10, initialWeights=np.array([1.0,1.0,1.0,1.0,1.0,1.0]))
print("HEYHO")
modellist = []
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00","12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    data = [int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),longitud,latitud]
    res1 = model.predict(np.array(data))
    # res2 = model2.predict(np.array(data))
    print(res1)
    modellist.append((time[0:2],res1))
    # model2list.append(res2)

print("KOM IGJEN")
print(modellist)
# print(model2list)

# last = calculated_list
# last.saveAsTextFile("BDA/output")
