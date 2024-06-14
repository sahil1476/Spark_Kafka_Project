from pyspark.sql import SparkSession

from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, DoubleType,FloatType, StructField, DateType,StringType


spark = SparkSession.builder.appName("CSV to Kafka").getOrCreate()

# Define the schema of the CSV file
schema = StructType([ 
    StructField("Date/Time", StringType(),True), 
    StructField("LV_ActivePower",DoubleType(),True), 
    StructField("Wind_Speed",DoubleType(),True), 
    StructField("Theoretical_Power_Curve", DoubleType(), True), 
    StructField("Wind_Direction", DoubleType(), True) 
  ])


# Read the CSV file as a stream
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/home/xs437-sahsha/Desktop/sahilsharma/dataset/T1.csv")\
 #.option("inferSchema","true")\       
df.show(10)    
#df.printSchema()
'''
root
 |-- Date/Time: date (nullable = true)
 |-- LV_ActivePower: double (nullable = true)
 |-- Wind_Speed: double (nullable = true)
 |-- Theoretical_Power_Curve: double (nullable = true)
 |-- Wind_Direction: double (nullable = true)
'''

# Write the DataFrame to Kafka
query = df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "final004") \
        .save()
     
#.option("checkpointLocation", "/home/xs437-sahsha/Desktop/sahilsharma/checkpoint") \
