package omnia;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import omnia.config.OmniaConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;

import java.util.Collections;
import java.util.Map;

public class TransactionToGameSessionTransformer {

    private TransactionToGameSessionAggregator aggregator;
    private TransactionToGameSessionMerger merger;

    public TransactionToGameSessionTransformer(){}

    public TransactionToGameSessionTransformer(TransactionToGameSessionAggregator aggregator, TransactionToGameSessionMerger merger) {
        this.aggregator = aggregator;
        this.merger = merger;
    }

    public KafkaStreams createStream() {
        final Serde<GenericRecord> keyGenericAvroSerde = getAvroRecordSerde(true);
        final Serde<GenericRecord> valueGenericAvroSerde = getAvroRecordSerde(false);
        final Serde<Windowed<GenericRecord>> windowedKeySerde = getWindowedAvroRecordSerde(true);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericRecord, GenericRecord> transactions = builder.stream(OmniaConfiguration.getTransactionsTopic());
        KStream<GenericRecord, GenericRecord> transactionsPerSession = transactions
                .filter((k,v) -> (Double) v.get(KafkaTopicKeys.AMOUNT) > 0.0)
                .map((k,v) ->  KeyValue.pair((GenericRecord) v.get(KafkaTopicKeys.TRANSACTION_KEY), v));
        getWindowedGenericRecordKTable(keyGenericAvroSerde, valueGenericAvroSerde, transactionsPerSession)
                .filter(((key, value) -> value != null),
                        Materialized.<Windowed<GenericRecord>, GenericRecord, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("filtered-sessionized-aggregation")
                                .withKeySerde(windowedKeySerde)
                                .withValueSerde(valueGenericAvroSerde))
                .to(windowedKeySerde, valueGenericAvroSerde, OmniaConfiguration.getSessionsTopic());
        return new KafkaStreams(builder.build(), new StreamsConfig(OmniaConfiguration.getKafkaConfig()));
    }

    private KTable<Windowed<GenericRecord>, GenericRecord> getWindowedGenericRecordKTable(Serde<GenericRecord> keyGenericAvroSerde,
                                                                                          Serde<GenericRecord> valueGenericAvroSerde,
                                                                                   KStream<GenericRecord, GenericRecord> sessionTransactions) {
        return sessionTransactions
                .groupByKey(Serialized.with(keyGenericAvroSerde, valueGenericAvroSerde))
                .windowedBy(SessionWindows.with(OmniaConfiguration.getSessionWindowGap())
                        .until(OmniaConfiguration.getSessionWindowDuration()))
                .aggregate(
                        () -> initializeSessionRecord(),
                        aggregator,
                        merger,
                        Materialized.<GenericRecord, GenericRecord, SessionStore<Bytes, byte[]>>as("sessionized-aggregation")
                                .withKeySerde(keyGenericAvroSerde)
                                .withValueSerde(valueGenericAvroSerde)
                        );
    }

    private Serde<GenericRecord> getAvroRecordSerde(final boolean isSerdeForRecordKeys) {
        return Serdes.serdeFrom(getAvroRecordSerializer(isSerdeForRecordKeys), getAvroRecordDeserializer(isSerdeForRecordKeys));
    }

    private Serde<Windowed<GenericRecord>> getWindowedAvroRecordSerde(final boolean isSerdeForRecordKeys) {
        Serde<Windowed<GenericRecord>> serde = Serdes.serdeFrom(new WindowedSerializer<>(getAvroRecordSerializer(isSerdeForRecordKeys)),
                new WindowedDeserializer<>(getAvroRecordDeserializer(isSerdeForRecordKeys)));
        serde.configure(getAvroSerdeConfig(), isSerdeForRecordKeys);
        return serde;
    }

    private Serializer getAvroRecordSerializer(final boolean isSerdeForRecordKeys) {
        Serializer serializer = new KafkaAvroSerializer();
        serializer.configure(getAvroSerdeConfig(), isSerdeForRecordKeys);
        return serializer;
    }

    private Deserializer getAvroRecordDeserializer(final boolean isSerdeForRecordKeys) {
        Deserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(getAvroSerdeConfig(), isSerdeForRecordKeys);
        return deserializer;
    }

    private Map<String, String> getAvroSerdeConfig() {
        return Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                OmniaConfiguration.getSchemaRegistryUrlConfig());
    }

    private GenericRecord initializeSessionRecord() {
        return (GenericRecord) GenericData.get()
                .newRecord(null, getSessionSchema());
    }

    private Schema getSessionSchema() {
        return new Schema.Parser().parse(OmniaConfiguration.getSessionsSchema());
    }

}
