def tableName = "test_streamload_action1"

sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        id int,
        name varchar(255)
    )
    DISTRIBUTED BY HASH(id) BUCKETS 1
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
    set 'column_separator', ','

    // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
    // also, you can stream load a http stream, e.g. http://xxx/some.csv
    file 'streamload_input.csv'

    time 10000 // limit inflight 10s

    // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
}


// stream load 100 rows
def rowCount = 100
// range: [0, rowCount)
// or rangeClosed: [0, rowCount]
def rowIt = range(0, rowCount)
        .mapToObj({i -> [i, "a_" + i]}) // change Long to List<Long, String>
        .iterator()

streamLoad {
    table tableName
    // also, you can upload a memory iterator
    inputIterator rowIt

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