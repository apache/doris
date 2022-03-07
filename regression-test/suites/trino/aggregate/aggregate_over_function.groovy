def tableName = "datetype"

sql """ DROP TABLE IF EXISTS ${tableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        c_bigint bigint,
        c_double double,
        c_string string,
        c_date date,
        c_timestamp datetime,
        c_boolean boolean,
        c_short_decimal decimal(5,2),
        c_long_decimal decimal(27,9)
    )
    DUPLICATE KEY(c_bigint)
    DISTRIBUTED BY HASH(c_bigint) BUCKETS 1
    PROPERTIES (
      "replication_num" = "1"
    )
"""

streamLoad {
    // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
    // db 'regression_test'
    table tableName

    // default label is UUID:
    // set 'label' UUID.randomUUID().toString()

    // default column_separator is specify in doris fe config, usually is '\t'.
    // this line change to ','
    set 'column_separator', '|'

    // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
    // also, you can stream load a http stream, e.g. http://xxx/some.csv
    file 'datetype.csv'

    time 10000 // limit inflight 10s

    // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

    // if declared a check callback, the default check condition will ignore.
    // So you must check all condition
    check { result, exception, startTime, endTime ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("success", json.Status.toLowerCase())
        assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
    }
}

qt_aggregate """ select max(upper(c_string)), min(upper(c_string)) from ${tableName} """
