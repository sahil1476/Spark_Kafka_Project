from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour,avg,countDistinct,when, broadcast
from pyspark.sql import Row

# Create SparkSession
spark = SparkSession.builder \
    .appName("operation") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# Path to the Delta table
delta_table_path = "/home/xs437-sahsha/Desktop/sahilsharma/delta001/location"

df = spark.read.format("delta").load(delta_table_path)
#==============================================================================================
# Task 
df.show()
distinct_per_day = df.groupBy("signal_date").agg(countDistinct("signal_tc").alias("distinct_signal_ts"))
 
distinct_per_day.show()
 
 
# Task  
df = df.withColumn("generation_indicator",when(col("signals.LV_ActivePower") < 200, "Low")\
    .when((col("signals.LV_ActivePower") >= 200) & (col("signals.LV_ActivePower") < 600), "Medium")\
    .when((col("signals.LV_ActivePower") >= 600) & (col("signals.LV_ActivePower") < 1000), "High")\
    .otherwise("Exceptional"))
 
df.printSchema()
df.show(10)

# Task 
json_data_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]

# Create DataFrame from JSON data
new_df = spark.createDataFrame([Row(**x) for x in json_data_df])
'''
# Show the DataFrame
new_df.show()
# Task  
# Perform broadcast join
broadcast_df = df.join(broadcast(new_df),
                       df.generation_indicator == new_df.sig_name,
                       "left_outer")
                       
# Update the 'generation_indicator' column
broadcast_df = broadcast_df.withColumn("generation_indicator", broadcast_df.sig_mapping_name)

# Drop columns
# broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")

# Show the updated DataFrame
broadcast_df.show()


'''