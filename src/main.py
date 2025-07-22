from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *

from hudi_config.cow_options import get_cow_options
from hudi_config.mor_options import get_mor_options

spark = SparkSession.builder \
    .appName("Hudi Ingest Clickstream") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# Hudi options
table_type = "mor"
hudi_table_name = "hudi_clickstream_" + table_type
hudi_table_path = "file:///Users/varun_asija/Documents/cdc-hudi-lakehouse/data/output/default/" + hudi_table_name   # or s3://your-bucket/hudi-table/
record_key = "user_id"
precombine_key = "timestamp"
partition_key = "event_date"

# checkpoint
checkpoint_path = "file:///Users/varun_asija/Documents/cdc-hudi-lakehouse/checkpoints"

# Define schema
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("country", StringType(), True),
])

# Read micro-batch JSON files (written by your data generator)
input_path = "data/raw"
df = (spark.readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1) # simulate micro-batch
      .json(input_path))

# Convert timestamp column to proper format
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Write to Hudi
hudi_options = None
if table_type == "cow":
    hudi_options = get_cow_options(hudi_table_name, record_key, partition_key, precombine_key)
elif table_type == "mor":
    hudi_options = get_mor_options(hudi_table_name, record_key, partition_key, precombine_key)

df.writeStream.format("hudi") \
    .options(**hudi_options) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='20 seconds') \
    .start(hudi_table_path) \
    .awaitTermination()

print(f"✅ Hudi table written to {hudi_table_path}")



"""
 spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0 \
  main.py
"""