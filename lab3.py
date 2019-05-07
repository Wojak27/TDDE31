import findspark
findspark.init()

#from future import division
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import datetime
from pyspark import SparkContext


if __name__ == "__main__":
    def kernelizeDate(line, date):
      print("Kernelize!", line)
      return line

    def kernelizeDistance(line):
      print(line[3], line[4])
      return exp(-(haversine(float(line[4]), float(line[3]), b, a))**2/h_distance)

    sc = SparkContext(appName="lab_kernel") 

    def haversine(lon1, lat1, lon2, lat2):
        """Calculate the great circle distance between two points on the earth (specified in decimal degrees)"""
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
        
        # haversine formula
        dlon = lon2 -lon1 
        dlat = lat2 -lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
        c = 2 * asin(sqrt(a))
        km = 6367 * c 
        print(km)
        return km



    h_distance = 10000# Up to you
    h_date  =  1#  Up  to  you
    h_time = 1# Up to you
    a = 60.4274 # Up to you
    b = 13.826 # Up to you
    date = "2013-07-04" # Up to you

    stations = sc.textFile("stations.csv").map(lambda x: x.split(";"))
    temps = sc.textFile("temps50k.csv")
    print("loading files succeeded")

    distance = stations.map(kernelizeDistance)

    print(distance.take(10))
