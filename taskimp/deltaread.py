# read from delta lake table............
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, countDistinct

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("deltatable") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# Create a DataFrame
df = spark.read\
    .format("delta") \
    .load("/home/xs437-sahsha/Desktop/sahilsharma/delta001/location") \
    .createOrReplaceTempView("location")

spark.sql("SELECT * FROM location").show(5)

spark.sql("SELECT signal_date FROM location").show(10)

spark.sql("SELECT signals.Theoretical_Power_Curve FROM location WHERE signals.Wind_Speed = 5.31133604049682 AND signals.Wind_Direction = 259.994903564453").show()

