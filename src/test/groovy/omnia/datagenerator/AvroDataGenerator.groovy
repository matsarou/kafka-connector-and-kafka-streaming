package omnia.datagenerator

import joptsimple.internal.Strings
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

class AvroDataGenerator {
    def schemaGenerator = new AvroSchemaGenerator()

    def generateTransactionKeyRecord(Map params) {
        GenericRecord keyRecord = GenericData.get()
                .newRecord(null, schemaGenerator.transactionsKeySchema)
        keyRecord.put(OmniaKeysForTest.SESSION, params['session'])
        keyRecord.put(OmniaKeysForTest.USER, params['userId'])
        keyRecord.put(OmniaKeysForTest.GAME, params['gameId'])
        keyRecord
    }

    def generateTransactionRecord(Map params) {
        GenericRecord transactionRecord = GenericData.get()
                .newRecord(null, schemaGenerator.transactionsSchema)
        transactionRecord.put(OmniaKeysForTest.DATE, params['date'])
        transactionRecord.put(OmniaKeysForTest.AMOUNT, params['amount'])
        transactionRecord.put(OmniaKeysForTest.BALANCE_BEFORE, params['balanceBefore'])
        transactionRecord.put(OmniaKeysForTest.BALANCE_AFTER, params['balanceAfter'])
        transactionRecord
    }

    def generateSessionRecord(Map params) {
        GenericRecord record = (GenericRecord) GenericData.get()
                .newRecord(null, schemaGenerator.sessionSchema)
        record.put(OmniaKeysForTest.SESSION_ID, params[OmniaKeysForTest.SESSION_ID])
        record.put(OmniaKeysForTest.USER, params[OmniaKeysForTest.USER])
        record.put(OmniaKeysForTest.GAME, params[OmniaKeysForTest.GAME])
        record.put(OmniaKeysForTest.STARTED, params[OmniaKeysForTest.STARTED])
        record.put(OmniaKeysForTest.ENDED, params[OmniaKeysForTest.ENDED])
        record.put(OmniaKeysForTest.TOTAL_DURATION_IN_SECONDS, params[OmniaKeysForTest.TOTAL_DURATION_IN_SECONDS])
        record.put(OmniaKeysForTest.TOTAL_WINNING_AMOUNT, params[OmniaKeysForTest.TOTAL_WINNING_AMOUNT])
        record.put(OmniaKeysForTest.TOTAL_WAGERING_AMOUNT, params[OmniaKeysForTest.TOTAL_WAGERING_AMOUNT])
        record.put(OmniaKeysForTest.TOTAL_WINS, params[OmniaKeysForTest.TOTAL_WINS])
        record.put(OmniaKeysForTest.TOTAL_WAGERS, params[OmniaKeysForTest.TOTAL_WAGERS])
        record
    }

    def getInitialSessionRecord() {
        GenericRecord record = (GenericRecord) GenericData.get()
                .newRecord(null, schemaGenerator.sessionSchema)
        record.put(OmniaKeysForTest.SESSION_ID, Strings.EMPTY)
        record.put(OmniaKeysForTest.TOTAL_DURATION_IN_SECONDS, 0)
        record.put(OmniaKeysForTest.TOTAL_WINNING_AMOUNT, Double.valueOf(0.0))
        record.put(OmniaKeysForTest.TOTAL_WAGERING_AMOUNT, Double.valueOf(0.0))
        record.put(OmniaKeysForTest.TOTAL_WINS, 0)
        record.put(OmniaKeysForTest.TOTAL_WAGERS, 0)
        record
    }


}

