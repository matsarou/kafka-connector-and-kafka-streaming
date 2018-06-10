# kafka-connector-and-kafka-streaming
Create a simple data pipeline, using the Confluent platform in compliance with Apache Avro schema.<br/><br/>

With kafka streams the data can be stored to a Kafka cluster in binary form, where the source can be a kafka topic and the target another one.<br/>

The data can also be stored to a database, by creating a Kafka Sink Connector
1) Retrieve and desserialize the data from the cluster, basing on an Avro schema
2) connect to the target database with Kafka Sink Connector
3) run or start the Connector, to transfer the data to the database tables

