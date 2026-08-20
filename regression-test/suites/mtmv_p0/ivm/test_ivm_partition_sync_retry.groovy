suite("test_ivm_partition_sync_retry", "nonConcurrent") {
    def skipPartitionSyncDebugPoint = "MTMVTask.syncPartitionsIfNeeded.skip"
    def skipPartitionSyncFilterDebugPoint = "MTMVTask.syncPartitionsIfNeeded.skip.filter"
    GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncFilterDebugPoint)
    GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncDebugPoint)

    sql """drop materialized view if exists ivm_partition_sync_retry_mv"""
    sql """drop table if exists ivm_partition_sync_retry_base"""

    sql """
        CREATE TABLE ivm_partition_sync_retry_base (
            id INT,
            dt DATE,
            v INT
        )
        UNIQUE KEY(id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p20260701 VALUES [('2026-07-01'), ('2026-07-02'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """INSERT INTO ivm_partition_sync_retry_base VALUES (1, '2026-07-01', 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_partition_sync_retry_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, dt, v FROM ivm_partition_sync_retry_base
    """

    sql """REFRESH MATERIALIZED VIEW ivm_partition_sync_retry_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_partition_sync_retry_mv")

    sql """ALTER TABLE ivm_partition_sync_retry_base
        ADD PARTITION p20260702 VALUES [('2026-07-02'), ('2026-07-03'))"""
    sql """INSERT INTO ivm_partition_sync_retry_base VALUES (2, '2026-07-02', 20)"""
    // Also change an existing partition so the stale refresh context selects an
    // IVM refresh scope; the new partition's binlog row then reaches the missing
    // MV partition and exercises the retry path.
    sql """INSERT INTO ivm_partition_sync_retry_base VALUES (1, '2026-07-01', 11)"""

    def taskResult
    try {
        // The first sync is skipped. The IVM insert sees the new binlog row before
        // the corresponding MV partition exists; the internal retry must sync it
        // and rerun IVM instead of falling back to PARTITIONS/COMPLETE.
        GetDebugPoint().enableDebugPointForAllFEs(
                skipPartitionSyncFilterDebugPoint, [mv_name: "ivm_partition_sync_retry_mv"])
        GetDebugPoint().enableDebugPointForAllFEs(skipPartitionSyncDebugPoint, [execute: 1])
        sql """REFRESH MATERIALIZED VIEW ivm_partition_sync_retry_mv INCREMENTAL FALLBACK"""
        waitingMTMVTaskFinishedByMvName("ivm_partition_sync_retry_mv")
        taskResult = sql_return_maparray("""
            SELECT Status, RefreshMode, IvmFallbackReason, ErrorMsg
            FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}'
              AND MvName = 'ivm_partition_sync_retry_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """)[0]
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncFilterDebugPoint)
        GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncDebugPoint)
    }

    assertEquals("SUCCESS", taskResult.Status.toString())
    assertTrue(taskResult.RefreshMode == null || taskResult.RefreshMode.toString() == "\\N")
    assertTrue(taskResult.IvmFallbackReason == null || taskResult.IvmFallbackReason.toString() == "\\N")
    order_qt_partition_sync_retry """
        SELECT dt, id, v FROM ivm_partition_sync_retry_mv
    """
}
