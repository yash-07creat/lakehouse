# gold_aggregates.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, sum as _sum, avg as _avg, count
import os

spark = SparkSession.builder \
    .appName("gold-aggregates") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

SILVER_PATH = os.path.abspath("storage/silver/rides_clean")
GOLD_PATH = os.path.abspath("storage/gold")

df = spark.read.format("delta").load(SILVER_PATH)

# Example 1: daily revenue
daily = (
    df.groupBy(date_trunc("day", "pickup_ts").alias("day"))
      .agg(_sum("fare_amount").alias("daily_revenue"), count("ride_id").alias("rides"))
      .orderBy("day")
)

daily.write.format("delta").mode("overwrite").save(os.path.join(GOLD_PATH, "daily_revenue"))

# Example 2: borough-level metrics
borough = (
    df.groupBy("pickup_borough")
      .agg(count("ride_id").alias("rides"), _avg("trip_distance").alias("avg_distance"), _avg("fare_amount").alias("avg_fare"))
)

borough.write.format("delta").mode("overwrite").save(os.path.join(GOLD_PATH, "borough_metrics"))

print("Wrote gold tables to", GOLD_PATH)

spark.stop()
