from hudi_config.base_hudi_options import get_base_hudi_options

def get_mor_options(hudi_table_name, record_key, partition_key, precombine_key):
    options = get_base_hudi_options(hudi_table_name, record_key, partition_key, precombine_key)
    options["hoodie.datasource.write.table.type"] = "MERGE_ON_READ"
    options["hoodie.datasource.compaction.async.enable"] = True # enabled by default
    options["hoodie.compact.inline.max.delta.commits"]  = 5
    options["hoodie.compaction.logfile.size.threshold"] = 1000 # in bytes
    return options
