def tableName = "test_streamload_performance1"

try {
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

    def rowCount = 10000
    def rowIt = java.util.stream.LongStream.range(0, rowCount)
            .mapToObj({i -> [i, "a_" + i]})
            .iterator()

    streamLoad {
        table tableName
        time 5000
        inputIterator rowIt
    }
} finally {
    try_sql "DROP TABLE IF EXISTS ${tableName}"
}