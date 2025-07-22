from hudi_config.base_hudi_options import get_base_hudi_options

def get_cow_options(hudi_table_name, record_key, partition_key, precombine_key):
    options = get_base_hudi_options(hudi_table_name, record_key, partition_key, precombine_key)
    options["hoodie.datasource.write.table.type"] = "COPY_ON_WRITE"
    return options
