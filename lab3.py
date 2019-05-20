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

    # This is the kernel for the distance 
    # We take the difference between points (distance),
    # divide by the smoothing factor and kernelize with 
    # exp(-abs(u)**2) (abs is not needed because of **2)
    def kernelizeDistance(line):
      return (int(line[0]), exp(-(haversine(float(line[4]), float(line[3]), b, a)/h_distance)**2))
  
    def kernelizeTime(line):
      # hours between times
      print("lol")
      

    def kernelizeDate(line):
      # splitting the date for the datetime
      splittedLine = line.split(";")
      splittedDate = splittedLine[1].split("-")
      # filter months and days that have 0 in front like 08 to 8
      month = splittedDate[1]
      day = splittedDate[2]
      if month[0] == 0:
          month = month[1]
      if day[0] == 0:
          day = day[1]
      currentDate = datetime(int(splittedDate[0]),int(splittedDate[1]),int(splittedDate[2]))
      difference = myDate - currentDate
      # print the day difference
      return (splittedLine[0], exp(-(difference.days/h_date)**2))
    
      
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
        return km



    h_distance = 100# Up to you
    h_date  =  100#  Up  to  you
    h_time = 1# Up to you
    a = 60.4274 # Up to you
    b = 13.826 # Up to you
    myDate = datetime(2008, 8, 18)  # Up to you
    
    stations = sc.textFile("stations.csv").map(lambda x: x.split(";"))
    temps = sc.textFile("temperature-readings.csv")

    distanceKernel = stations.map(kernelizeDistance)
    
    daysKernel = temps.map(kernelizeDate)

    # Broadcast the values for distance and date
    distanceKernel = distanceKernel.collectAsMap()
    #bcDistance = sc.broadcast(distance)

    daysKernel = daysKernel.collectAsMap()

#bcDays = sc.broadcast(days)

#temps = temps.map(lambda x: (x, bcDays.value[x[0]], bcDistance.value[x[0]]))

#   time = 0
#   arrayMulti = []
#   while currTime < 24:
#       arrayMulti.append(temps.map(calcTotMultiKernel))
#       time += 2


# def calcTotMultiKernel(line):
#   currTemp = line[-3]
#   currKernDate = line[-2]
#   currKernDist = line[-1]
#   currKernTime = exp(-(int((line[-4].split(:)[0])-time)/h_time) ** 2)
#   return currTemp * currKernDate * currKernDist * currKernTime

#   time = 0
#   arrayAdd = []
#   while currTime < 24:
#       arrayAdd.append(temps.map(calcTotAddKernel))
#       time += 2


# def calcTotAddKernel(line):
#   currTemp = line[-3]
#   currKernDate = line[-2]
#   currKernDist = line[-1]
#   currKernTime = exp(-(int((line[-4].split(:)[0])-time)/h_time) ** 2)
#   return currTemp * (currKernDate + currKernDist + currKernTime)


#print(distance.take(10))
print(temps.take(10))

