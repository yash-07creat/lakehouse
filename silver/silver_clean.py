# silver_clean.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("silver-clean") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_TABLE_PATH = os.path.abspath("storage/bronze/rides_table")
SILVER_PATH = os.path.abspath("storage/silver/rides_clean")

# read bronze
df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

# cleaning rules: drop nulls in key fields, drop unrealistic distances, deduplicate
cleaned = (
    df.where(col("pickup_ts").isNotNull())
      .where(col("trip_distance") > 0)
      .dropDuplicates(["ride_id"])
)

# enrich: add hour/day columns
from pyspark.sql.functions import hour
cleaned2 = cleaned.withColumn("pickup_hour", hour(col("pickup_ts")))

cleaned2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_PATH)

print("Wrote silver table to", SILVER_PATH)

spark.stop()
