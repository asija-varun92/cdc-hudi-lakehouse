from fastavro import reader
import json

avro_file = "data/output/default/products/.hoodie/timeline/20250925203230913_20250925203231754.commit"  # or HoodieCommitMetadata.avro
json_file = "output.json"

with open(avro_file, "rb") as f:
    avro_reader = reader(f)
    records = [record for record in avro_reader]

# Write to JSON
with open(json_file, "w") as f:
    json.dump(records, f, indent=2)