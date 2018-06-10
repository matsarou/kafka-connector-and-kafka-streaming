package omnia.datagenerator

import org.apache.avro.SchemaBuilder

class AvroSchemaGenerator {

    def getTransactionsKeySchema() {
        SchemaBuilder
                .record('TransactionKey')
                .fields()
                .requiredString(OmniaKeysForTest.SESSION)
                .requiredInt(OmniaKeysForTest.USER)
                .requiredInt(OmniaKeysForTest.GAME)
                .endRecord()
    }

    def getTransactionsSchema() {
        SchemaBuilder
                .record('transaction')
                .fields()
                .requiredString(OmniaKeysForTest.SESSION)
                .requiredInt(OmniaKeysForTest.USER)
                .requiredInt(OmniaKeysForTest.GAME)
                .requiredString(OmniaKeysForTest.DATE)
                .optionalDouble(OmniaKeysForTest.AMOUNT)
                .requiredDouble(OmniaKeysForTest.BALANCE_BEFORE)
                .requiredDouble(OmniaKeysForTest.BALANCE_AFTER)
                .endRecord()
    }

    def getSessionSchema() {
        SchemaBuilder
                .record('session')
                .fields()
                .requiredString(OmniaKeysForTest.SESSION_ID)
                .requiredString(OmniaKeysForTest.USER)
                .requiredString(OmniaKeysForTest.GAME)
                .requiredString(OmniaKeysForTest.STARTED)
                .requiredString(OmniaKeysForTest.ENDED)
                .requiredDouble(OmniaKeysForTest.TOTAL_DURATION_IN_SECONDS)
                .requiredDouble(OmniaKeysForTest.TOTAL_WINNING_AMOUNT)
                .requiredDouble(OmniaKeysForTest.TOTAL_WAGERING_AMOUNT)
                .requiredInt(OmniaKeysForTest.TOTAL_WINS)
                .requiredInt(OmniaKeysForTest.TOTAL_WAGERS)
                .endRecord()
    }
}
