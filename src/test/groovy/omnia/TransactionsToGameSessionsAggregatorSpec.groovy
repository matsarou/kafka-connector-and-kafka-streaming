package omnia

import omnia.datagenerator.AvroDataGenerator
import org.apache.avro.generic.GenericRecord
import spock.lang.Specification

class TransactionsToGameSessionsAggregatorSpec extends Specification {

    TransactionToGameSessionAggregator aggregator
    AvroDataGenerator dataGenerator

    def setup() {
        aggregator = new TransactionToGameSessionAggregator()
        dataGenerator = new AvroDataGenerator()
    }

    def "Create two sessions from three transactions, two of which have the same record-key"() {
        given: "a union of ids as key of the aggregated record"
        GenericRecord aggKey1 = dataGenerator.generateTransactionKeyRecord([
                session: 'session1',
                userId: 2,
                gameId: 10310
        ])
        GenericRecord aggKey2 = dataGenerator.generateTransactionKeyRecord([
                session:'session2',
                userId:2,
                gameId:10310
        ])
        and: "the first coming Transaction record in the session with key equal to the aggKey"
        GenericRecord transaction1 = dataGenerator.generateTransactionRecord([
                TransactionKey: aggKey1,
                date         : '2018-02-27T13:10:25.9964775',
                amount       : 1.5,
                balanceBefore: Double.valueOf(73.37),
                balanceAfter : Double.valueOf(73.27)]
        )
        and: "the second coming Transaction record in the session with key equal to the aggKey"
        GenericRecord transaction2 = dataGenerator.generateTransactionRecord([
                TransactionKey: aggKey1,
                date         : '2018-02-27T13:12:25.9964775',
                amount       : 3.4,
                balanceBefore: Double.valueOf(73.27),
                balanceAfter : Double.valueOf(74.57)]
        )
        and: "the third coming Transaction record in the session with key non-equal to the aggKey"
        GenericRecord transaction3 = dataGenerator.generateTransactionRecord([
                TransactionKey: aggKey2,
                date         : '2018-02-27T15:22:25.9964775',
                amount       : 3.4,
                balanceBefore: Double.valueOf(74.57),
                balanceAfter : Double.valueOf(34.57)]
        )
        and: "a generic initial Session record, when the session1 and session2 are created"
        GenericRecord initialSession1 = dataGenerator.initialSessionRecord
        GenericRecord initialSession2 = dataGenerator.initialSessionRecord

        when: "the first transaction1 comes, it gets in the session1"
        GenericRecord session1 = aggregator.apply(aggKey1, transaction1, initialSession1)
        and: "the second transaction2 comes, it gets in the session1"
        aggregator.apply(aggKey1, transaction2, session1)
        and: "the third transaction2 comes, it gets in the session2"
        GenericRecord session2 = aggregator.apply(aggKey2, transaction3, initialSession2)

        then:
        assertRecord(session1, ['session1-2-10310', '2018-02-27T13:10:25.996477500', '2018-02-27T13:12:25.996477500', 1.3, 0.1, 1, 1, 120.0,'2','10310'])
        assertRecord(session2, ['session2-2-10310', '2018-02-27T15:22:25.996477500', '2018-02-27T15:22:25.996477500', 0, 40, 0, 1, 0.0,'2','10310'])
    }

    void assertRecord(GenericRecord record, List params) {
        assert record != null
        assert params[0].equals(record.get('session_id').toString())
        assert params[1].equals(record.get('started').toString())
        assert params[2].equals(record.get('ended').toString())
        assert params[3] == record.get('total_winning_amount')
        assert params[4] == record.get('total_wagering_amount')
        assert params[5] == record.get('total_wins')
        assert params[6] == record.get('total_wagers')
        assert params[7] == record.get('total_duration_in_seconds')
        assert params[8] == record.get('userId')
        assert params[9] == record.get('gameId')
    }

}