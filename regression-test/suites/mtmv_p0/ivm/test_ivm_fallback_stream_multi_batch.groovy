// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_ivm_fallback_stream_multi_batch", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def forcedFallbackDebugPoint = "IvmRefreshManager.doRefresh.force_fallback_reason"

    GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)

    sql """drop materialized view if exists ivm_fbs_mb_mv;"""
    sql """drop table if exists ivm_fbs_o;"""
    sql """drop table if exists ivm_fbs_c;"""

    sql """
        CREATE TABLE ivm_fbs_o (
            order_id INT,
            dt INT,
            cid INT,
            amount INT
        )
        UNIQUE KEY(order_id, dt)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20240101 VALUES LESS THAN ("20240102"),
            PARTITION p20240102 VALUES LESS THAN ("20240103")
        )
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_fbs_c (
            cid INT,
            name VARCHAR(20)
        )
        UNIQUE KEY(cid)
        PARTITION BY RANGE(cid)
        (
            PARTITION p100 VALUES LESS THAN ("100"),
            PARTITION p200 VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(cid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    def waitVisible = {
        sql "sync"
        sleep(1200)
    }

    sql """
        INSERT INTO ivm_fbs_c VALUES
            (1, 'alice'),
            (101, 'bob');
    """
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (10, 20240101, 1, 10),
            (20, 20240102, 101, 20);
    """
    waitVisible()

    def listIvmStreamNames = {
        sql("""
            SELECT DISTINCT STREAM_NAME
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}'
            ORDER BY STREAM_NAME
        """).collect { row -> row[0].toString() }
                .findAll { streamName -> streamName.startsWith("__doris_ivm_stream_") }
                .toSet()
    }
    def streamNamesBeforeMvCreate = listIvmStreamNames()

    sql """
        CREATE MATERIALIZED VIEW ivm_fbs_mb_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'refresh_partition_num' = '1'
        )
        AS
        SELECT
            ivm_fbs_o.dt AS dt,
            ivm_fbs_o.order_id AS order_id,
            ivm_fbs_o.cid AS cid,
            ivm_fbs_o.amount AS amount,
            ivm_fbs_c.name AS name
        FROM ivm_fbs_o
        INNER JOIN ivm_fbs_c
            ON ivm_fbs_o.cid = ivm_fbs_c.cid;
    """

    def ivmStreamNames = (listIvmStreamNames() - streamNamesBeforeMvCreate).toSet()
    assertEquals(2, ivmStreamNames.size())

    def queryBaseRows = {
        sql """SET enable_materialized_view_rewrite=false"""
        try {
            sql """
                SELECT
                    CAST(o.dt AS INT),
                    CAST(o.order_id AS INT),
                    CAST(o.cid AS INT),
                    CAST(o.amount AS INT),
                    CAST(c.name AS VARCHAR(20))
                FROM ivm_fbs_o o
                INNER JOIN ivm_fbs_c c
                    ON o.cid = c.cid
                ORDER BY o.dt, o.order_id
            """
        } finally {
            sql """SET enable_materialized_view_rewrite=true"""
        }
    }
    def queryMvRows = {
        sql """
            SELECT dt, order_id, cid, amount, name
            FROM ivm_fbs_mb_mv
            ORDER BY dt, order_id
        """
    }
    def assertMvEqualsBase = {
        assertEquals(queryBaseRows().toString(), queryMvRows().toString())
    }
    def assertLatestRefreshTask = { expectedMode, expectedFallbackReason, expectedPartitionCount ->
        def tasks = sql """
            SELECT RefreshMode, IvmFallbackReason,
                   JSON_LENGTH(NeedRefreshPartitions), JSON_LENGTH(CompletedPartitions), Progress
            FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}'
              AND MvName = 'ivm_fbs_mb_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """
        assertEquals(1, tasks.size())
        assertEquals(expectedMode, tasks[0][0].toString())
        def fallbackReason = tasks[0][1] == null ? null : tasks[0][1].toString()
        assertEquals(expectedFallbackReason, fallbackReason == "\\N" ? null : fallbackReason)
        assertEquals(expectedPartitionCount.toString(), tasks[0][2].toString())
        assertEquals(expectedPartitionCount.toString(), tasks[0][3].toString())
        assertEquals("100.00% (${expectedPartitionCount}/${expectedPartitionCount})".toString(),
                tasks[0][4].toString())
    }
    def queryStreamConsumption = {
        def streamNameList = ivmStreamNames.toList().sort()
                .collect { streamName -> "'${streamName}'" }
                .join(", ")
        sql """
            SELECT STREAM_NAME, UNIT, LAG
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}'
              AND STREAM_NAME IN (${streamNameList})
            ORDER BY STREAM_NAME, UNIT
        """
    }
    def assertStreamRows = { consumptionRows ->
        assertEquals(4, consumptionRows.size())
        assertEquals(ivmStreamNames, consumptionRows.collect { row -> row[0].toString() }.toSet())
        assertEquals(["p100", "p200", "p20240101", "p20240102"].toSet(),
                consumptionRows.collect { row -> row[1].toString() }.toSet())
    }
    def assertLagUnits = { expectedLagUnits ->
        def consumptionRows = queryStreamConsumption()
        assertStreamRows(consumptionRows)
        def actualLagUnits = consumptionRows.findAll { row -> row[2].toString() != "0" }
                .collect { row -> row[1].toString() }.toSet()
        assertEquals(expectedLagUnits.toSet(), actualLagUnits)
    }
    def assertStreamLagZero = {
        assertLagUnits([])
    }

    // A PARTITIONS request on an uninitialized MV covers every MV partition and must
    // reset the shared non-PCT stream offset in the first batch.
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv PARTITIONS"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("COMPLETE", null, 2)
    assertMvEqualsBase()
    assertStreamLagZero()

    // A zero-lag COMPLETE must still rebuild both non-empty MV partitions.
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("COMPLETE", null, 2)
    assertMvEqualsBase()
    assertStreamLagZero()

    sql """INSERT INTO ivm_fbs_o VALUES (11, 20240101, 1, 15);"""
    waitVisible()
    assertLagUnits(["p20240101"])

    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint,
                [reason: "PLAN_PATTERN_UNSUPPORTED"])
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("PARTIAL", "PLAN_PATTERN_UNSUPPORTED", 1)
        assertMvEqualsBase()
        assertStreamLagZero()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }

    sql """INSERT INTO ivm_fbs_o VALUES (12, 20240101, 1, 18);"""
    waitVisible()
    assertLagUnits(["p20240101"])
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertMvEqualsBase()
    assertStreamLagZero()

    // A non-PCT change affects every MV partition, so fallback must use COMPLETE and
    // commit the shared non-PCT offset after refreshing both batches.
    sql """
        INSERT INTO ivm_fbs_c VALUES
            (2, 'carol'),
            (102, 'dave');
    """
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (14, 20240101, 2, 28),
            (21, 20240102, 102, 25);
    """
    waitVisible()
    assertLagUnits(["p100", "p200", "p20240101", "p20240102"])
    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint,
                [reason: "PLAN_PATTERN_UNSUPPORTED"])
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("COMPLETE", "PLAN_PATTERN_UNSUPPORTED", 2)
        assertMvEqualsBase()
        assertStreamLagZero()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }

    sql """INSERT INTO ivm_fbs_c VALUES (3, 'erin');"""
    waitVisible()
    assertLagUnits(["p100"])
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertMvEqualsBase()
    assertStreamLagZero()

    sql """INSERT INTO ivm_fbs_o VALUES (13, 20240102, 3, 19);"""
    waitVisible()
    assertLagUnits(["p20240102"])
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertMvEqualsBase()
    assertStreamLagZero()
}
