package omnia;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaStreamsLoader {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsLoader.class);

    public static void main(String[] args) {
        TransactionToGameSessionAggregator aggregator = new TransactionToGameSessionAggregator();
        TransactionToGameSessionMerger merger = new TransactionToGameSessionMerger();
        TransactionToGameSessionTransformer transformer = new TransactionToGameSessionTransformer(aggregator,merger);
        KafkaStreams streams = transformer.createStream();
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            LOGGER.error("Unxpected error occured" + throwable.toString());
        });
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
