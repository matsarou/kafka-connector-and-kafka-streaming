package omnia

import omnia.datagenerator.AvroDataGenerator
import org.apache.avro.generic.GenericRecord
import spock.lang.Specification

class TransactionToGameSessionMergerSpec extends Specification {

    TransactionToGameSessionMerger merger
    AvroDataGenerator dataGenerator

    def setup() {
        merger = new TransactionToGameSessionMerger()
        dataGenerator = new AvroDataGenerator()
    }


    def "merge two omnia sessions"() {
        given: "two Avro records"
        GenericRecord session1 = dataGenerator.generateSessionRecord([
                session_id: 'key5-3-10310',
                userId: '3',
                gameId: '10310',
                started: '2018-02-27T11:15:25.996477500',
                ended: '2018-02-27T19:15:25.996477500',
                total_duration_in_seconds: Double.valueOf(28800.0),
                total_winning_amount: Double.valueOf(0.7),
                total_wagering_amount: Double.valueOf(0.30000000000000004),
                total_wins: 7,
                total_wagers: 3
        ])
        GenericRecord session2 = dataGenerator.generateSessionRecord([
                session_id: 'key5-3-10310',
                userId: '3',
                gameId: '10310',
                started: '2018-02-27T12:15:25.996477500',
                ended: '2018-02-27T20:15:25.996477500',
                total_duration_in_seconds: Double.valueOf(30800.0),
                total_winning_amount: Double.valueOf(1.33),
                total_wagering_amount: Double.valueOf(0.454),
                total_wins: 4,
                total_wagers: 100
        ])
        and: "the record key for the aggregation"
        GenericRecord aggKey1 = dataGenerator.generateTransactionKeyRecord([
                session: 'session1',
                userId: 2,
                gameId: 10310
        ])

        when:
        GenericRecord mergedRecord = merger.apply(aggKey1, session1, session2)

        then:
        mergedRecord != null
        assertRecord(mergedRecord, ['key5-3-10310', '3', '10310', '2018-02-27T11:15:25.996477500', '2018-02-27T20:15:25.996477500', 30800.0, 2.0300000000000002, 0.754, 11, 103])
    }

    void assertRecord(GenericRecord record, List params) {
        assert record != null
        assert params[0].equals(record.get('session_id').toString())
        assert params[1] == record.get('userId')
        assert params[2] == record.get('gameId')
        assert params[3].equals(record.get('started').toString())
        assert params[4].equals(record.get('ended').toString())
        assert params[5] == record.get('total_duration_in_seconds')
        assert params[6] == record.get('total_winning_amount')
        assert params[7] == record.get('total_wagering_amount')
        assert params[8] == record.get('total_wins')
        assert params[9] == record.get('total_wagers')


    }
}
