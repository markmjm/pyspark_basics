import os
import pyspark
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType,
                               IntegerType, StructType)
from pyspark.sql.functions import (countDistinct, avg, stddev, format_number, mean, round)
from pyspark.sql.functions import (dayofmonth, dayofweek, dayofyear,corr,
                                   hour, month, year, weekofyear, format_number, date_format)

##
os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jre1.8.0_144'
print(bool(os.environ["JAVA_HOME"]))  # True or False
print(bool(os.environ["SPARK_HOME"]))  # True or False
print(os.environ["JAVA_HOME"])  # print JAVA_HOME path
print(os.environ["SPARK_HOME"])  # print "SPARKE_HOME" path
##

spark = SparkSession.builder.appName('project').getOrCreate()
df = spark.read.csv('walmart_stock.csv', inferSchema=True, header=True)
print(df.columns)
df.printSchema()
df.describe().show()
df.select(['Date', 'Open', 'High', 'Low', 'Close']).show()
# print top 5 rows
print(df.head(5))
# print last row (can't use tail)
sc = spark.sparkContext
sqlContext = SQLContext(sc)
df.createOrReplaceTempView("table_df")
query_last_rec = """SELECT * FROM table_df ORDER BY Date DESC limit 1"""
last_rec = sqlContext.sql(query_last_rec)
last_rec.show()
# print first 5 row using sqlContext
query_top_5_reccords = """SELECT * FROM table_df ORDER BY Date limit 5"""
top_5_reccords = sqlContext.sql(query_top_5_reccords)
top_5_reccords.show()
#
#foramting ... all fields are string
df.describe().printSchema()
result = df.describe()
result.select(result['summary'],
              format_number(result['Open'].cast('float'),2).alias('Open'),
              format_number(result['High'].cast('float'), 2).alias('High'),
              format_number(result['Low'].cast('float'), 2).alias('Low'),
              format_number(result['Close'].cast('float'), 2).alias('Close'),
              format_number(result['Volume'].cast('float'), 2).alias('Volume'),
              format_number(result['Adj Close'].cast('float'), 2).alias('Adj Close')
              ).show()

# #Get min/max Vol
# df.select(max('Volume'),min('Volume')).show()
#
#spark df ops
#
df2 = df.withColumn('HV Ratio', df['High']/df['Volume'])
df2.select('HV Ratio').show()
#
#Get High price
print(df.orderBy(df['High'].desc()).head(1))
df.agg({'High':'max'}).alias('MAX High').show()
#
#Get mean clsing price
df.agg({'Close':'mean'}).alias('AVG CLOSE').show()
#
#Filter
print(df.filter('Close < 60').count())
print(df.filter(df['Close'] < 60).count())
#
#What percentage of the time was the High greater than 80 dollars ?
percentage = (df.filter(df['High'] > 80).count() / df.count()) * 100
print(percentage)
#
#What is the Pearson correlation between High and Volume?
df.select(corr('High','Volume')).show()
#
#What is the max High per year?
yeardf = df.withColumn('Year', year(df['Date']))
max_df = yeardf.groupBy('Year').max()
max_df.select('Year', 'Max(High)').show()
#
#What is the average Close for each Calendar Month?
monthdf = df.withColumn('Month', month('Date'))
avg_df = monthdf.select(['Month', 'Close']).groupBy('Month').mean()
avg_df.select('Month', 'avg(Close)').orderBy('Month').show()