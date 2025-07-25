from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from utils.hudi_writer import write_to_hudi

spark = SparkSession.builder \
    .appName("Hudi Ingest Clickstream") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

table_type = "mor"

# input data
input_servers = "localhost:29092"
kafka_topic = "inventory.inventory.products,inventory.inventory.customers"

base_path = "file:///Users/varun_asija/Documents/cdc-hudi-lakehouse/data/output/default/"

# Read from Kafka
messages_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", input_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

raw_df = messages_df.select(col("topic"), col("value").cast("string").alias("str_data"))

# Write to Hudi
raw_df.writeStream \
    .foreachBatch(lambda df, bid: write_to_hudi(df, bid, table_type, base_path)) \
    .option("checkpointLocation", f"checkpoints/multi-topic-stream") \
    .trigger(processingTime='20 seconds') \
    .start() \
    .awaitTermination()


"""
 spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  main.py
"""