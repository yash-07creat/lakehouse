# bronze_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os

spark = SparkSession.builder \
    .appName("bronze-transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_PATH = os.path.abspath("storage/bronze/rides")
BRONZE_TABLE_PATH = os.path.abspath("storage/bronze/rides_table")

# load raw bronze
df = spark.read.format("delta").load(BRONZE_PATH)

# Standardize: parse timestamps, enforce types
df2 = (
    df
    .withColumn("pickup_ts", to_timestamp(col("pickup_ts")))
    .withColumn("dropoff_ts", to_timestamp(col("dropoff_ts")))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
)

# write to a managed bronze table (path)
df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(BRONZE_TABLE_PATH)

print("Bronze transform finished ->", BRONZE_TABLE_PATH)
spark.stop()
