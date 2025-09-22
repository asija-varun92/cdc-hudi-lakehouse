install:
	schematool -initSchema -dbType derby
	hive --service metastore > metastore.log 2>&1 &

run:
	spark-submit src/main.py

query:
	spark-submit query_data.py

clean:
	rm -rf metastore_db/ metastore.log derby.log checkpoints/multi-topic-stream data/output/default/*
