from pyspark.sql.types import *

table_config = {
    "products" : {
        "record_key" : "id",
        "partition_key"  : "name",
        "precombine_key" : "ts_ms",
        "schema" : StructType([
                        StructField("id", IntegerType(), False),
                        StructField("name", StringType(), False),
                        StructField("description", StringType(), True),
                        StructField("weight", DoubleType(), True)
                    ])
    },
    "customers" : {
            "record_key" : "id",
            "partition_key"  : "first_name",
            "precombine_key" : "ts_ms",
            "schema" : StructType([
                            StructField("id", IntegerType(), False),
                            StructField("first_name", StringType(), False),
                            StructField("last_name", StringType(), True),
                            StructField("email", StringType(), True)
                        ])
        }
}