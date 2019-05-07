import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
# Import SparkSession
from pyspark.sql import SparkSession

print("Hello World")

# Build the SparkSession
spark = SparkSession.builder \
   .master("local") \
   .appName("Linear Regression Model") \
   .config("spark.executor.memory", "1gb") \
   .getOrCreate()

sc = spark.sparkContext
sqlContext=SQLContext(sc)

# Load in the data
f = sc.textFile("/Users/karolwojtulewicz/Documents/PySpark/temps50k.csv")\

parts = f.map(lambda l:l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0], date=p[1], month=p[1].split("-")[1], year=p[1].split("-")[0], time=p[2], value=float(p[3]), quality=p[4]))


schemaTempReadings =  sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")
#print(sqlContext.sql("SELECT year, MAX(value), MIN(value) FROM (SELECT year, month, value, station FROM tempReadings WHERE year IN (SELECT DISTINCT year FROM tempReadings) AND month IN (SELECT DISTINCT month FROM tempReadings)) WHERE year IN (SELECT DISTINCT year from tempReadings) GROUP BY year").show())

# First Assignment
#print(sqlContext.sql("SELECT year, month, station, MAX(value) as MAX_Value FROM tempReadings WHERE year> '1949' AND year<'2015' GROUP BY year, month, station ORDER BY -MAX_Value").show())
#print(sqlContext.sql("SELECT year, month, station, MIN(value) as MIN_Value FROM tempReadings WHERE year> '1949' AND year<'2015' GROUP BY year, month, station ORDER BY MIN_Value").show())


# Second Assignment

# Without the Distinct station
#print(sqlContext.sql("SELECT year, month, COUNT(value) as count FROM tempReadings WHERE year> '1949' AND year<'2015' AND value>10 GROUP BY year, month ORDER BY -count").show())

# With the Distinct station
#print(sqlContext.sql("SELECT year, month, COUNT(value) as count FROM (SELECT DISTINCT year,month,station,value FROM tempReadings WHERE value>10) WHERE year> '1949' AND year<'2015' GROUP BY year, month ORDER BY count").show())

# Third assignment

#print(sqlContext.sql("SELECT year, month, station, AVG(value) as AVG_Value FROM tempReadings WHERE year> '1959' AND year<'2015' GROUP BY year, month, station ORDER BY AVG_Value").show())


# Fourth assignment
