from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveTableCheck") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# List all databases
# spark.sql("SHOW DATABASES").show()

# Switch to a database (default is the default)
spark.sql("USE default")

spark.sql("DROP TABLE IF EXISTS products;")

# List tables
spark.sql("SHOW TABLES").show()

# # View schema of a specific table
# spark.sql("DESCRIBE FORMATTED products").show(truncate=False)
