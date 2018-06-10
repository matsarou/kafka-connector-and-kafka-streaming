package omnia;

import omnia.helper.Util;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Aggregator;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

public class TransactionToGameSessionAggregator implements Aggregator<GenericRecord, GenericRecord, GenericRecord> {

    @Override
    public GenericRecord apply(final GenericRecord aggKey, final GenericRecord transaction, final GenericRecord producedSession) {
        configureSessionRecordKeyInfo(aggKey, producedSession);
        LocalDateTime dateFromRecord = LocalDateTime.parse(transaction.get(KafkaTopicKeys.DATE).toString());
        configureSessionRecordStartedDate(producedSession, dateFromRecord);
        configureSessionRecordEndedDate(producedSession, dateFromRecord);
        configureSessionRecordDuration(producedSession);
        double balanceDiff = findBalanceAmountDiff(Util.getDoubleValueOrDefault(transaction.get(KafkaTopicKeys.BALANCE_BEFORE)),
                Util.getDoubleValueOrDefault(transaction.get(KafkaTopicKeys.BALANCE_AFTER)));
        configureSessionRecordWinnings(producedSession, balanceDiff);
        configureSessionRecordWagering(producedSession, balanceDiff);
        return producedSession;
    }

    private void configureSessionRecordKeyInfo(GenericRecord recordKey, GenericRecord producedSession) {
        String seddionId = String.format("%s-%s-%s", recordKey.get(KafkaTopicKeys.SESSION), recordKey.get(KafkaTopicKeys.USER),
                recordKey.get(KafkaTopicKeys.GAME));
        producedSession.put(KafkaTopicKeys.SESSION_ID, seddionId);
        producedSession.put(KafkaTopicKeys.USER, recordKey.get(KafkaTopicKeys.USER).toString());
        producedSession.put(KafkaTopicKeys.GAME, recordKey.get(KafkaTopicKeys.GAME).toString());
    }

    private void configureSessionRecordStartedDate(GenericRecord record, LocalDateTime dateFromRecord) {
        try {
            LocalDateTime started = LocalDateTime.parse(record.get(KafkaTopicKeys.STARTED).toString());
            if (dateFromRecord != null && started.isAfter(dateFromRecord)) {
                record.put(KafkaTopicKeys.STARTED, dateFromRecord.toString());
            }
        } catch (Exception exc) {
            record.put(KafkaTopicKeys.STARTED, dateFromRecord.toString());
        }
    }

    private void configureSessionRecordEndedDate(GenericRecord record, LocalDateTime dateFromRecord) {
        try {
            LocalDateTime ended = LocalDateTime.parse(record.get(KafkaTopicKeys.ENDED).toString());
            if(dateFromRecord != null && ended.isBefore(dateFromRecord)) {
                record.put(KafkaTopicKeys.ENDED, dateFromRecord.toString());
            }
        } catch (Exception exc) {
            record.put(KafkaTopicKeys.ENDED, dateFromRecord.toString());
        }
    }

    private void configureSessionRecordDuration(GenericRecord record) {
        LocalDateTime started = LocalDateTime.parse(record.get(KafkaTopicKeys.STARTED).toString());
        LocalDateTime ended = LocalDateTime.parse(record.get(KafkaTopicKeys.ENDED).toString());
        Duration duration = Duration.between(started, ended);
        record.put(KafkaTopicKeys.TOTAL_DURATION_IN_SECONDS, Double.valueOf(Long.toString(duration.getSeconds())));
    }

    private double findBalanceAmountDiff(double balanceBefore, double balanceAfter) {
        return (BigDecimal.valueOf(balanceAfter).subtract(BigDecimal.valueOf(balanceBefore))).doubleValue();
    }

    private void configureSessionRecordWinnings(GenericRecord record, double balanceDiff) {
        double winningAmountOrDefault = Util.getDoubleValueOrDefault(record.get(KafkaTopicKeys.TOTAL_WINNING_AMOUNT));
        int winsOrDefault = Util.getIntValueOrDefault(record.get(KafkaTopicKeys.TOTAL_WINS));
        if(balanceDiff > 0) {
            record.put(KafkaTopicKeys.TOTAL_WINNING_AMOUNT, winningAmountOrDefault + balanceDiff);
            record.put(KafkaTopicKeys.TOTAL_WINS, ++ winsOrDefault);
        } else {
            record.put(KafkaTopicKeys.TOTAL_WINNING_AMOUNT, winningAmountOrDefault);
            record.put(KafkaTopicKeys.TOTAL_WINS, winsOrDefault);
        }
    }

    private void configureSessionRecordWagering(GenericRecord record, double balanceDiff) {
        double wageringAmountOrDefault = Util.getDoubleValueOrDefault(record.get(KafkaTopicKeys.TOTAL_WAGERING_AMOUNT));
        int wagersOrDefault = Util.getIntValueOrDefault(record.get(KafkaTopicKeys.TOTAL_WAGERS));
        if(balanceDiff < 0) {
            record.put(KafkaTopicKeys.TOTAL_WAGERING_AMOUNT, wageringAmountOrDefault - balanceDiff);
            record.put(KafkaTopicKeys.TOTAL_WAGERS, ++ wagersOrDefault);
        } else {
            record.put(KafkaTopicKeys.TOTAL_WAGERING_AMOUNT, wageringAmountOrDefault);
            record.put(KafkaTopicKeys.TOTAL_WAGERS, wagersOrDefault);
        }
    }
}

