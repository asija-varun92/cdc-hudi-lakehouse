# Setup Postgres Database, Debezium and Kafka connect for CDC

## Using docker

1. Run below command from root folder
    `docker-compose up -d`
2. Register Topic with below command
    ```
   curl -X POST http://localhost:8083/connectors \
   -H "Content-Type: application/json" \
   -d '{
    "name": "inventory-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "inventory",
      "database.server.name": "dbserver1",
      "plugin.name": "pgoutput",
      "slot.name": "debezium",
      "publication.name": "debezium_pub",
      "topic.prefix": "inventory",
      "snapshot.mode": "initial"
    }}'
   ```
3. Connect to db with creds below:
   ```
   host - localhost
   db - inventory
   user - postgres
   password - postgres
   ```
4. consume messages with below command
   ```
   docker run -it --rm --network=cdc-hudi-lakehouse_default \
   confluentinc/cp-kafka:7.3.0 \
   kafka-console-consumer \
   --bootstrap-server kafka:9092 \
   --topic inventory.inventory.products \
   --from-beginning
   ```


# Manual setup 

1. ✅ Install Kafka and Zookeeper
Download from Apache Kafka

Extract and enter Kafka folder

`tar -xzf kafka_2.13-<version>.tgz`

`cd kafka_2.13-<version>`

Start Zookeeper:

`bin/zookeeper-server-start.sh config/zookeeper.properties`

Start Kafka:

`bin/kafka-server-start.sh config/server.properties`


2. ✅ Start Kafka Connect with Debezium Connector
Kafka Connect comes with Kafka (since 0.10+).

Download Debezium Connector plugin:

For PostgreSQL:
https://mvnrepository.com/artifact/io.debezium/debezium-connector-postgres

Extract the JARs to a folder, e.g., connectors/debezium-postgres/

Then run Kafka Connect:
bin/connect-distributed.sh config/connect-distributed.properties

Update connect-distributed.properties to include your plugin path:

`plugin.path=/absolute/path/to/connectors`


3. ✅ Enable CDC on the Database
For PostgreSQL:
In postgresql.conf, set:


```
wal_level = logical
max_replication_slots = 1
max_wal_senders = 10
```

Also, ensure your user has replication privileges:

`ALTER ROLE varun_asija WITH REPLICATION;`


4. ✅ Register Debezium Connector via REST API
Once Kafka Connect is running, register a connector:

```
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-inventory-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "localhost",
      "database.port": "5432",
      "database.user": "varun_asija",
      "database.password": "",
      "database.dbname": "postgres",
      "topic.prefix": "varun_db1",
      "plugin.name": "pgoutput",
      "slot.name": "debezium",
      "publication.name": "debezium_pub"
    }
  }'
```


You should now see Kafka topics like:

`{"name":"pg-inventory-connector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"localhost","database.port":"5432","database.user":"varun_asija","database.password":"","database.dbname":"postgres","topic.prefix":"varun_db1","plugin.name":"pgoutput","slot.name":"debezium","publication.name":"debezium_pub","name":"pg-inventory-connector"},"tasks":[],"type":"source"}`


5. ✅ Consume Topics
```
kafka_2.12-3.5.0 % bin/kafka-console-consumer.sh \                             
  --bootstrap-server localhost:9092 \
  --topic varun_db1.public.products \
  --from-beginning
```
