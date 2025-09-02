def get_base_hudi_options(hudi_table_name, record_key, partition_key, precombine_key):
    return {
    "hoodie.table.name": hudi_table_name,
    "hoodie.datasource.write.recordkey.field": record_key,
    "hoodie.datasource.write.partitionpath.field": partition_key,
    "hoodie.datasource.write.precombine.field": precombine_key,
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.hive_style_partitioning": "true",

    # ✅ Bucket indexing hudi_config
    "hoodie.index.type": "BUCKET",
    "hoodie.bucket.index.num.buckets": "2",
    "hoodie.bucket.index.hash.field": record_key,

    # ✅ Hive Sync options
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",  # Use HMS directly (recommended)
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": hudi_table_name,
    "hoodie.datasource.hive_sync.partition_fields": partition_key,
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",

    # ✅ Data Cleaning service options
    "hoodie.clean.policy": "KEEP_LATEST_COMMITS",
    "hoodie.clean.commits.retained": 2

}