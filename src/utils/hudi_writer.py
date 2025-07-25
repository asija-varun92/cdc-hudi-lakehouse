from pyspark.sql.functions import col, from_json

from table_configs import table_config

from hudi_config.cow_options import get_cow_options
from hudi_config.mor_options import get_mor_options
from utils.get_message_schema import get_message_schema

def write_to_hudi(batch_df, batch_id, table_type, base_path):

    topic_names = [row.topic for row in batch_df.select("topic").distinct().collect()]

    for topic_name in topic_names:
        hudi_table_name = topic_name.split(".")[2]
        tb_config = table_config[hudi_table_name]
        table_schema = tb_config["schema"]
        message_schema = get_message_schema(table_schema)
        table_df = batch_df.filter(col("topic") == topic_name).drop("topic")
        parsed_df = table_df.select(from_json(col("str_data"), message_schema).alias("data"))
        data_df = parsed_df.select("data.payload.after.*", "data.payload.op", "data.payload.ts_ms")

        table_path = f"{base_path}/{hudi_table_name}"

        hudi_options = None
        if table_type == "cow":
            hudi_options = get_cow_options(hudi_table_name, record_key=tb_config["record_key"],
                                           partition_key=tb_config["partition_key"],
                                           precombine_key=tb_config["precombine_key"])
        elif table_type == "mor":
            hudi_options = get_mor_options(hudi_table_name, record_key=tb_config["record_key"],
                                           partition_key=tb_config["partition_key"],
                                           precombine_key=tb_config["precombine_key"])

        data_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(table_path)
