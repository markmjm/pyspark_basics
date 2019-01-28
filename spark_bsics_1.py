import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import ( StructField, StringType,
                                IntegerType,StructType)
##
os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jre1.8.0_144'
print(bool(os.environ["JAVA_HOME"])) # True or False
print(bool(os.environ["SPARK_HOME"])) # True or False
print(os.environ["JAVA_HOME"]) # print JAVA_HOME path
print(os.environ["SPARK_HOME"]) # print "SPARKE_HOME" path
##
spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('people.json')
print(f'---df.show():')
df.show()
print(f'---df.printSchema():')
df.printSchema()
print(f'---df.columns:')
print(df.columns)
print(f'---df.describe().show()')
df.describe().show()
#
# Sometimes you may need to clarify to Spark what the schema looks like, especially for large DS and may not be clear
# from pyspark.sql.types import ( StructField, StringType,
#                                 IntegerType,StructType)
data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]  # True => field can be null
final_struct = StructType(fields=data_schema)
# now read people.json again ... this time define the schema
# note hoe age is now integer, not long
df = spark.read.json('people.json', schema=final_struct)
df.printSchema()

import findspark
##findspark.init(os.environ["SPARK_HOME"])
#df = spark.sql('''select 'spark' as hello ''')
#df.show()