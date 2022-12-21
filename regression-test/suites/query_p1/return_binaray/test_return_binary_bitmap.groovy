suite("test_return_binary_bitmap") {
    def tableName="test_return_binary_bitmap"
    sql "drop table if exists ${tableName};"

    sql """
    CREATE TABLE `${tableName}` (
        `dt` int(11) NULL,
        `page` varchar(10) NULL,
        `user_id` bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `page`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`dt`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """
        insert into ${tableName} values(1,1,to_bitmap(1)),(1,1,to_bitmap(2)),(1,1,to_bitmap(3)),(1,1,to_bitmap(23332));
    """
    sql "set enable_vectorized_engine=true;"
    sql "set return_object_data_as_binary=false;"
    def result1 = sql "select * from ${tableName}"
    assertTrue(result1[0][2]==null);

    sql "set enable_vectorized_engine=true;"
    sql "set return_object_data_as_binary=true;"
    def result2 = sql "select * from ${tableName}"
    assertTrue(result2[0][2]!=null);
}