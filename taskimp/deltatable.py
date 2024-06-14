
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,split,substr,struct, to_date, current_date, current_timestamp,to_timestamp,date_format
from pyspark.sql.types import StructType,StructField, DoubleType , DateType, TimestampType, MapType, FloatType,StringType

#SparkSession
spark = SparkSession.builder \
    .appName("deltatable") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.streaming.checkpointLocation", "/home/xs437-sahsha/Desktop/sahilsharma/new_check_point")\
    .getOrCreate()

# Define Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "final003") \
    .option("startingOffsets", "earliest") \
    .load()
df.printSchema()  

json_df = df.selectExpr("cast(value as string) as value")
json_schema =  StructType([ 
    StructField("Date/Time", StringType(),False), 
    StructField("LV_ActivePower",DoubleType(),True), 
    StructField("Wind_Speed",DoubleType(),True), 
    StructField("Theoretical_Power_Curve", DoubleType(), True), 
    StructField("Wind_Direction", DoubleType(), True) 
  ])
# Apply Schema to JSON value column and expand the value
from pyspark.sql.functions import from_json

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 
  
if json_expanded_df.isStreaming:
    print("\n DataFrame is streaming. Monitoring for new data...\n")
else:
    print("\nDataFrame is not streaming. No new data to monitor.\n")

'''query = json_expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
        .awaitTermination()'''

signals_map = {
    "LV_ActivePower": "LV_ActivePower",
    "Wind_Speed": "Wind_Speed",
    "Theoretical_Power_Curve": "Theoretical_Power_Curve",
    "Wind_Direction": "Wind_Direction"
}

update_df = json_expanded_df.withColumn('signal_date',to_timestamp(split(json_expanded_df['Date/Time'],' ').getItem(2)))\
    .withColumn('signal_tc',to_timestamp(split(json_expanded_df['Date/Time'],' ').getItem(3)))\
    .withColumn("create_date",date_format(current_date(), 'dd MM yyyy'))\
    .withColumn("create_ts",date_format(current_timestamp(), 'dd MM yyyy HH:mm:ss'))  \
    .withColumn("signals", struct([json_expanded_df[col].alias(col) for col in signals_map]))\
        .drop("Date/Time","LV_ActivePower","Wind_Speed","Theoretical_Power_Curve","Wind_Direction")         

#delta table..
query = update_df.writeStream \
    .format("delta") \
        .outputMode("append")\
        .option("mergeSchema", "true")\
       .start("/home/xs437-sahsha/Desktop/sahilsharma/delta001/location")
query.awaitTermination()          

