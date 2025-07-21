from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *

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
hudi_table_path = "file:///Users/varun_asija/Documents/cdc-hudi-lakehouse/data/output/default/hudi_clickstream"   # or s3://your-bucket/hudi-table/
hudi_table_name = "hudi_clickstream"
record_key = "user_id"
precombine_key = "timestamp"
partition_key = "event_date"

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
      .option("maxFilesPerTrigger", 3) # simulate micro-batch
      .json(input_path))

# Convert timestamp column to proper format
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Write to Hudi
hudi_options = {
    "hoodie.table.name": hudi_table_name,
    "hoodie.datasource.write.recordkey.field": record_key,
    "hoodie.datasource.write.partitionpath.field": partition_key,
    "hoodie.datasource.write.precombine.field": precombine_key,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.hive_style_partitioning": "true",

    # ✅ Bucket indexing config
    "hoodie.index.type": "BUCKET",
    "hoodie.bucket.index.num.buckets": "2",
    "hoodie.bucket.index.hash.field": record_key,

    # ✅ Hive Sync options
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",  # Use HMS directly (recommended)
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": hudi_table_name,
    "hoodie.datasource.hive_sync.partition_fields": "event_date",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083"
}

df.writeStream.format("hudi") \
    .options(**hudi_options) \
    .outputMode("append") \
    .option("checkpointLocation", "file:///Users/varun_asija/Documents/cdc-hudi-lakehouse/checkpoints/clickstream_hudi") \
    .trigger(processingTime='20 seconds') \
    .start(hudi_table_path) \
    .awaitTermination()

print(f"✅ Hudi table written to {hudi_table_path}")



"""
 spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0 \
  main.py
"""