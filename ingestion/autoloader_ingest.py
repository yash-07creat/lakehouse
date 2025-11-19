# autoloader_ingest.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os

spark = SparkSession.builder \
    .appName("lakehouse-autoloader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

RAW_DIR = os.path.abspath("data/raw")
BRONZE_PATH = os.path.abspath("storage/bronze/rides")
CHECKPOINT = BRONZE_PATH + "_checkpoint"

# read all json files from data/raw
raw_df = spark.read.option("multiline", "false").json(RAW_DIR)

# add provenance column
raw_df = raw_df.withColumn("_source_file", input_file_name())

# write as Delta
raw_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(BRONZE_PATH)

print("Wrote Bronze delta to:", BRONZE_PATH)

spark.stop()
