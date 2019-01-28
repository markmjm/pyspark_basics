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
# grab a column
age = df.select('age')
print(f'Age: Type: {type(age)}')
df.select('age').show()
# grab 2 rows
age_rows = df.head(2)[0]
print(f'Age_rows: Type: {type(age_rows)}')
print(df.head(2))
print(df.head(2)[0])
# grab multiple cols
cols = df.select(['age','name'])
print(f'Cols: Type: {type(cols)}')
df.select(['age','name']).show()
# Add column
new_df = df.withColumn('newage', df['age']* 2.0).show()
# Rename Column
df.withColumnRenamed('age', 'my_new_age').show()
#
#pure sql
df.createOrReplaceTempView('people') # this is now a registered view of the table
results = spark.sql("Select * from people")
results.show()
new_results = spark.sql("Select * from people where age <30")
new_results.show()


