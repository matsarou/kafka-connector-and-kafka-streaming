package omnia;

import joptsimple.internal.Strings;
import omnia.config.OmniaConfiguration;
import omnia.helper.Util;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Merger;

public class TransactionToGameSessionMerger implements Merger<GenericRecord, GenericRecord> {

    @Override
    public GenericRecord apply(GenericRecord aggKey, GenericRecord leftAggValue, GenericRecord rightAggValue) {
        return getMergedSessionRecord(leftAggValue, rightAggValue);
    }

    private GenericRecord getMergedSessionRecord(GenericRecord record1, GenericRecord record2) {
        GenericRecord newRecord = (GenericRecord) GenericData.get()
                .newRecord(null, getSessionSchema());
        newRecord.put(KafkaTopicKeys.SESSION_ID, getSessionRecordProperty(record1.get(KafkaTopicKeys.SESSION_ID),
                record2.get(KafkaTopicKeys.SESSION_ID)));
        newRecord.put(KafkaTopicKeys.USER, getSessionRecordProperty(record1.get(KafkaTopicKeys.USER),
                record2.get(KafkaTopicKeys.USER)));
        newRecord.put(KafkaTopicKeys.GAME, getSessionRecordProperty(record1.get(KafkaTopicKeys.GAME),
                record2.get(KafkaTopicKeys.GAME)));
        newRecord.put(KafkaTopicKeys.TOTAL_WINS, getTotalWins(record1, record2));
        return newRecord;
    }

    private int getTotalWins(GenericRecord record1, GenericRecord record2) {
        return Integer.sum(Util.getIntValueOrDefault(record1.get(KafkaTopicKeys.TOTAL_WINS)),
                Util.getIntValueOrDefault(record2.get(KafkaTopicKeys.TOTAL_WINS)));
    }

    private String getSessionRecordProperty(Object obj1, Object obj2) {
        if(obj1 != null) {
            return obj1.toString();
        } else if(obj2 != null) {
            return obj2.toString();
        } else {
            return Strings.EMPTY;
        }
    }

    private Schema getSessionSchema() {
        return new Schema.Parser().parse(OmniaConfiguration.getSessionsSchema());
    }

}
