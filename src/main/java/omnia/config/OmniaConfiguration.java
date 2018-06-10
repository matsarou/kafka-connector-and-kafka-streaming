package omnia.config;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class OmniaConfiguration {
    private static Configuration configuration = buildFromConfig("kafka.config.properties");

    public static Properties getKafkaConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, configuration.getString("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getString("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, configuration.getString("schema.registry.url"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configuration.getString("auto.offset.reset"));
        return props;
    }

    public static String getSchemaRegistryUrlConfig() {
        return configuration.getString("schema.registry.url");
    }

    public static String getTransactionsTopic() {
            return configuration.getString("transactions.topic");
    }

    public static String getSessionsTopic() {
        return configuration.getString("sessions.topic");
    }

    public static String getSessionsSchema() {
        return configuration.getString("sessions.avro.schema");
    }

    public static int getSessionWindowDuration() {
        try {
            return Integer.parseInt(configuration.getString("session.window.duration.ms"));
        } catch(NumberFormatException exc) {
            return 0;
        }
    }

    public static int getSessionWindowGap() {
        try {
            return Integer.parseInt(configuration.getString("session.window.gap.ms"));
        } catch(NumberFormatException exc) {
            return 0;
        }
    }

    private static Configuration buildFromConfig(String configFileName) {
        try {
            List<FileLocationStrategy> locationStrategies = Arrays.asList(new ProvidedURLLocationStrategy(),
                    new FileSystemLocationStrategy(),
                    new ClasspathLocationStrategy());
            FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                    .configure(new Parameters().fileBased()
                            .setLocationStrategy(new CombinedLocationStrategy(locationStrategies))
                            .setFileName(configFileName));
            return builder.getConfiguration();
        } catch (ConfigurationException e1) {
            throw new RuntimeException("Unable to load config", e1);
        }
    }
}
