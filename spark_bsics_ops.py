import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import ( StructField, StringType,
                                IntegerType,StructType)
from pyspark.sql.functions import (countDistinct, avg, stddev, format_number, mean)
from pyspark.sql.functions import (dayofmonth,dayofweek, dayofyear,
                                   hour, month, year, weekofyear,format_number,date_format)
##
os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jre1.8.0_144'
print(bool(os.environ["JAVA_HOME"])) # True or False
print(bool(os.environ["SPARK_HOME"])) # True or False
print(os.environ["JAVA_HOME"]) # print JAVA_HOME path
print(os.environ["SPARK_HOME"]) # print "SPARKE_HOME" path
##
spark = SparkSession.builder.appName('ops').getOrCreate()
df = spark.read.csv('appl_stock.csv', inferSchema=True,header=True)
print(f'---df.show():')
df.show()
print(f'---df.printSchema():')
df.printSchema()
# using dataframe ops (could have used sql)
# select all close < 500
df.filter('Close < 500').show()
# select all close < 500 and dsiplay 2 cols
df.filter('Close < 500').select(['Open', 'Close']).show()
# select all close < 500 and dsiplay 2 cols ... using pandas like ops
df.filter(df['Close'] < 500).select(['Open', 'Close']).show()
# select w multiple conditions
df.filter( (df['Close'] < 200) & (df['Open'] > 200) ).select(['Open', 'Close']).show()
df.filter( (df['Close'] < 200) & ~(df['Open'] > 200) ).select(['Open', 'Close']).show()
# select specifc row, using collect
result = df.filter(df['Low'] > 500 ).select(['Open', 'Close']).collect()
print(result)
rows = [row for row in result]
dict = rows[0].asDict()
print(f'Dict: {dict}')
#################
## GROUP BY
#################
df_sales = spark.read.csv('sales_info.csv', inferSchema=True,header=True)
df_sales.show()
df_sales.printSchema()
print('df_sales.groupBy("Company").mean():')
df_sales.groupBy('Company').mean().show()
print('df_sales.groupBy("Company").sum():')
df_sales.groupBy('Company').sum().show()
#################
## AGGREGATE
# #################
df_sales.agg({'Sales':'sum'}).show()
df_sales.agg({'Sales':'max'}).show()
df_sales.agg({'Sales':'min'}).show()
df_sales.agg({'Sales':'count'}).show()
#
# OTHER OPS
df_sales.select(countDistinct('Sales').alias('Average Sales')).show()
df_sales.select(avg('Sales')).show()
df_sales.select(stddev('Sales')).show()
sales_std = df_sales.select(stddev('Sales').alias('std'))
sales_std.select(format_number('std',2).alias('std')).show()
df_sales.orderBy(df_sales['Sales'].desc()).show()
##################
# MISSING VALUES
##################
df_missing = spark.read.csv('ContainsNull.csv',  inferSchema=True,header=True)
df_missing.show()
df_missing.na.drop().show()
df_missing.na.drop(thresh=2).show()
#
#using how
df_missing.na.drop(how='all').show()
df_missing.na.drop(how='any').show()
#
#using subset
df_missing.na.drop(subset=['Sales']).show()
##################
# FILL VALUES
##################
df_missing.na.fill(0).show() # fills numberic fields that are null
df_missing.na.fill('This was null').show() # fills text fields that are null
df_missing.na.fill('Was Null Name',subset=['Name'] ).show() # fills text fields that are null
mean_val = df_missing.select(mean(df_missing['Sales'])).collect()[0][0]
print(mean_val)
df.na.fill(mean_val,['Sales']).show()
# or
df_missing.na.fill( df_missing.select(mean(df_missing['Sales'])).collect()[0][0], ['Sales']).show()
##################
# DATES and TIMESTAMP
##################
df_appl = spark.read.csv('appl_stock.csv',  inferSchema=True,header=True)
df_appl.select(dayofmonth(df_appl['Date'])).show()