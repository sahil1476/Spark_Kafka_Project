from pyspark.sql import SparkSession

from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, DoubleType,FloatType, StructField, DateType, StringType


spark = SparkSession.builder.appName("readfromkafka").getOrCreate()

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "final004") \
    .option("startingOffsets", "earliest") \
    .load()

#df.printSchema()

# Parse value from binay to string
json_df = df.selectExpr("cast(value as string) as value")
json_schema =  StructType([ 
    StructField("Date/Time", StringType(),True), 
    StructField("LV_ActivePower",DoubleType(),True), 
    StructField("Wind_Speed",DoubleType(),True), 
    StructField("Theoretical_Power_Curve", DoubleType(), True), 
    StructField("Wind_Direction", DoubleType(), True) 
  ])
# Apply Schema to JSON value column and expand the value
from pyspark.sql.functions import from_json

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

query = json_expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()   
    