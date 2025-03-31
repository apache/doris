suite("test_stream_load_illegal_timeout", "p0") {
    def tableName = "test_stream_load_illegal_timeout";

   def be_num = sql "show backends;"
    if (be_num.size() > 1) {
        // not suitable for multiple be cluster.
        return
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    
    streamLoad {
        table "${tableName}"
        set 'column_separator', '\t'
        set 'label', 'test_stream_load_illegal_timeout'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'strict_mode','true'

        file 'large_test_file.csv'
        set 'http_timeout' 'abc'
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)

            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.toLowerCase().contains("timeout") ||
                        json.Message.toLowerCase().contains("invalid") ||
                        json.Message.toLowerCase().contains("illegal"))
        } 
    }

}