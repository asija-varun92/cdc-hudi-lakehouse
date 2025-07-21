from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveTableCheck") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# List all databases
spark.sql("SHOW DATABASES").show()

# Switch to a database (default is the default)
spark.sql("USE default")

# List tables
# spark.sql("SHOW TABLES").show()
spark.sql("SELECT user_id, event_type, timestamp, session_id, page_url, country, event_date FROM hudi_clickstream;").show()

# # View schema of a specific table
# spark.sql("DESCRIBE FORMATTED hudi_clickstream").show(truncate=False)
