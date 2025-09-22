from pyspark.sql.types import *


def get_message_schema(table_schema):

    # Schema for "payload" block
    payload_schema = StructType([
        StructField("before", table_schema, False),
        StructField("after", table_schema, False),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
    ])

    # Final schema for the complete message
    message_schema = StructType([
        StructField("payload", payload_schema, True)
    ])
    return message_schema