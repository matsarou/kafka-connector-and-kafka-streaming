# kafka-connector-and-kafka-streaming
A simple data pipeline which uses the Confluent platform with the Apache Avro.<br/>

# Kafka streams aggregation with session windowing 
This is a real-time application that processes binary data stored in Kafka cluster. <br/>
The Avro records of an input topic are grouped into a session window with configured inactivity gap. They are grouped by an aggregate-key and each group produces one Avro record to the output topic.<br/>
In this example, I put the aggregate-key to be a composition of the attributes {session_id,userId,gameId}.<br/>
<br/>
## Build the application:
1)Start the kafka cluster(start in sequence the zookeeper, kafka broker, schema registry). <br/>
./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties<br/>
./bin/kafka-server-start ./etc/kafka/server.properties<br/>
./bin/schema-registry-start ./etc/schema-registry/connect-avro-standalone.properties
<br/>
2)Prepare the input topic with the source data and the output topic, where the aggregated data will be stored.<br/>
./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic transactions<br/>
./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sessions<br/>
./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic transactions --property value.schema='{"type":"record","name":"transaction","namespace":"kafka.avro.example","fields":[{"name":"TransactionKey","type":{"type":"record","name":"TransactionKeyRecord","fields":[{"name":"session_id","type":"string"},{"name":"userId","type":"string"},{"name":"gameId","type":"string"}]}},{"name": "amount","type": "double"}]}'<br/>
{"TransactionKey":{"session_id":"001","userId":"45678","gameId":"003"},"amount":5.3}
<br/>
3)Execute build command:<br/>
mvn clean package

## Run the application:
Open intellij or eclipse and run the main class KafkaStreamsLoader

## Test the application:
Consume the output topic: <br/>
/bin/kafka-avro-console-consumer --topic sessions --zookeeper localhost:2181 --from-beginning <br/>
You should see the aggregated record:<br/>
{"session_id":"001-45678-003","userId":"45678","gameId":"003","total_win_amount":5.3,"total_wins":1}
<br/>
Kafka Sink Connector<br/>
The aggregated data will be stored to a database.<br/>
1)Create the database with the tables, in compliance with the Avro schema transactions.avro.schema
2)start the Kafka Sink Connector



