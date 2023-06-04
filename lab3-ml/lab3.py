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
def gaussian_kernel(x,h):
  x = x/h
  K = exp(-(pow(x,2)))
  return K
def distanceDiff(longitud1, latitud1, longitud2, latitud2,h_distance):
  distance = haversine(longitud1, latitud1,longitud2,latitud2)
  K = gaussian_kernel(distance, h_distance)
  return K

def dateDiff(date1, date2, h_date):
    date1 = datetime(int(float(date1[0:4])),int(float(date1[5:7])),int(float(date1[8:10])))
    date2 = datetime(int(float(date2[0:4])),int(float(date2[5:7])),int(float(date2[8:10])))
    diff = date1 - date2
    days = diff.days
    days = days % 365
    if(days < 182.5):
        days = days
    else:
        days = 365 - days
    K = gaussian_kernel(days, h_date)
    return K

def timeDiff(time1, time2, h_time):
  difference = abs(int(time1[0:2])-int(time2[0:2]))
  K = gaussian_kernel(difference, h_time)
  return K


h_distance = 300000
h_date = 15
h_time = 3
longitud = 58.4274 # Up to youl
latitud  = 14.826 # Up to you
date = "2013-01-01" # Up to you
stations = sc.textFile("BDA/input/stations.csv")
stations = stations.map(lambda line: line.split(";"))
temps = sc.textFile("BDA/input/temperature-readings.csv")
#temps = temps.sample(False, 0.1)
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
# ((u'114410', u'1998-06-01', u'11:00:00'), (u'3.4', (61.6606, 14.9948)))
st_list.unpersist()
sum_list = []
mult_list = []
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00","12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    # Your code here
    # (temp) (datekernel, timekernel, distancekernel)

    ######################################################################
    # COMMENT-3 ADDED HOURLY FILTER FOR TIMES BEFORE THE TIME WE WANT TO PREDICT!
    merged_temp = merged_temp.filter(lambda x: ((int(x[0][2][0:2]) < int(time[0:2])) & (int(x[0][1][0:4]) <= int(date[0:4]))&(int(x[0][1][5:7]) <= int(date[5:7])) & (int(x[0][1][8:10]) <= int(date[8:10]))))
    merged_temp = merged_temp.cache() # ADDED CACHE COMMENT 5
    ######################################################################

    kernels = merged_temp.map(lambda x: ((x[1][0]), (dateDiff(date,x[0][1],h_date), timeDiff(x[0][2],time, h_time),distanceDiff(longitud,latitud,x[1][1][0],x[1][1][1], h_distance)) ) )
    ####### SUM ########
    sum = kernels.map(lambda x: (1, ( (float(x[0]) * (x[1][0] + x[1][1] + x[1][2])), (x[1][0] + x[1][1] + x[1][2])) ))
    summ = sum.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    predSum=summ.mapValues(lambda x: (x[0]/x[1])).collectAsMap()
    sum_list.append((time, predSum.get(1)))

    ####### MULT ########
    mult = kernels.map(lambda x: (1, ( (float(x[0]) * (x[1][0] * x[1][1] * x[1][2])), (x[1][0] * x[1][1] * x[1][2])) ))
    multiply_Added = mult.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    predmultiply=multiply_Added.mapValues(lambda x: (x[0]/x[1])).collectAsMap()
    mult_list.append((time, predmultiply.get(1)))

print("THE OUTPUT: ")
print(sum_list)
print(mult_list)

# last = st_list
# last.saveAsTextFile("BDA/output")
