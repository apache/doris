suite("test_sync_table") {
    def tableName = "test_sync_table_tbl"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` INT,
            `name` STRING
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "group_commit_interval_ms" = "10000" -- 10s
        );
    """

    sql """set group_commit = async_mode;"""

    // 插入数据后，不等待直接查，结果为空
    sql """INSERT INTO ${tableName} VALUES (1, "tom"), (2, "jerry");"""

    qt_select1 """SELECT COUNT(*) FROM ${tableName};"""

    // 执行sync table
    sql """SYNC TABLE ${tableName};"""

    // 数据已经写入
    qt_select2 """SELECT COUNT(*) FROM ${tableName};"""
}
