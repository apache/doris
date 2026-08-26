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

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.NodeType

suite("test_single_replica_ingest_binlog") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_single_replica_ingest_binlog")
        return
    }

    def tableName = "tbl_single_replica_ingest_binlog"
    def insert_num = 5

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    assertTrue(syncer.getTargetMeta("${tableName}"))

    logger.info("=== Test 1: Single replica ingest binlog case ===")
    for (int index = 0; index < insert_num; index++) {
        sql """INSERT INTO ${tableName} VALUES (1, ${index})"""
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.ingestBinlogSingleReplica())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        syncer.closeBackendClients()
    }

    target_sql "sync"
    def res = target_sql """SELECT * FROM ${tableName} WHERE test=1 ORDER BY id"""
    assertEquals(res.size(), insert_num)

    logger.info("=== Test 2: Idempotent re-ingest for the same txn ===")
    sql """INSERT INTO ${tableName} VALUES (2, 0)"""
    assertTrue(syncer.getBinlog("${tableName}"))
    assertTrue(syncer.beginTxn("${tableName}"))
    assertTrue(syncer.getBackendClients())
    // First ingest should succeed.
    assertTrue(syncer.ingestBinlogSingleReplica())
    // Re-ingest the same txn with the same load_id should be idempotent.
    assertTrue(syncer.ingestBinlogSingleReplica())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT * FROM ${tableName} WHERE test=2"""
    assertEquals(res.size(), 1)

    logger.info("=== Test 3: Multi-bucket table with empty rowsets ===")
    def bucketTableName = "tbl_single_replica_ingest_binlog_buckets"
    sql "DROP TABLE IF EXISTS ${bucketTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${bucketTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    sql """ALTER TABLE ${bucketTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${bucketTableName}"
    target_sql """
        CREATE TABLE IF NOT EXISTS ${bucketTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    assertTrue(syncer.getTargetMeta("${bucketTableName}"))

    // Insert values that hash to only some buckets, leaving other tablets with empty rowsets.
    sql """INSERT INTO ${bucketTableName} VALUES (1, 10), (2, 20)"""
    assertTrue(syncer.getBinlog("${bucketTableName}"))
    assertTrue(syncer.beginTxn("${bucketTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlogSingleReplica())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT * FROM ${bucketTableName} ORDER BY k"""
    assertEquals(res.size(), 2)

    logger.info("=== Test 4: MOW table idempotent re-ingest (delete bitmap must not be overwritten) ===")
    def mowTableName = "tbl_single_replica_ingest_binlog_mow"
    sql "DROP TABLE IF EXISTS ${mowTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${mowTableName}
        (
            `k` INT,
            `v` STRING
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    sql """ALTER TABLE ${mowTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${mowTableName}"
    target_sql """
        CREATE TABLE IF NOT EXISTS ${mowTableName}
        (
            `k` INT,
            `v` STRING
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    assertTrue(syncer.getTargetMeta("${mowTableName}"))

    // Phase 1: establish baseline and publish it.
    sql """INSERT INTO ${mowTableName} VALUES (1, '10'), (2, '20')"""
    assertTrue(syncer.getBinlog("${mowTableName}"))
    assertTrue(syncer.beginTxn("${mowTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlogSingleReplica())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT k, v FROM ${mowTableName} ORDER BY k"""
    assertEquals(res.size(), 2)
    assertEquals(res[0][0], 1)
    assertEquals(res[0][1], '10')
    assertEquals(res[1][0], 2)
    assertEquals(res[1][1], '20')

    // Phase 2: update an existing key and then ingest the same txn twice.
    // A reliable multi-segment source rowset is hard to construct in regression
    // (MOW deduplicates keys in the memtable and debug points are unavailable in
    // release builds), so the strict rowset-id bitmap coverage is deferred to a
    // BE unit test. Here we still verify the idempotent ingest flow and data.
    sql """INSERT INTO ${mowTableName} SELECT 2, repeat('x', 1000000) FROM numbers("number" = "100")"""

    assertTrue(syncer.getBinlog("${mowTableName}"))
    assertTrue(syncer.beginTxn("${mowTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlogSingleReplica())
    assertTrue(syncer.ingestBinlogSingleReplica())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT k, v FROM ${mowTableName} ORDER BY k"""
    assertEquals(res.size(), 2)
    assertEquals(res[0][0], 1)
    assertEquals(res[0][1], '10')
    assertEquals(res[1][0], 2)
    // The large value for k=2 should be visible; if the bitmap were overwritten by
    // a non-existent R2 rowset id, the row would be hidden or the query would fail.
    assertEquals(res[1][1].length(), 1000000)

    logger.info("=== Test 5: Legacy multi-replica ingest binlog path regression ===")
    def legacyTableName = "tbl_single_replica_ingest_binlog_legacy"
    sql "DROP TABLE IF EXISTS ${legacyTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${legacyTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    sql """ALTER TABLE ${legacyTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${legacyTableName}"
    target_sql """
        CREATE TABLE IF NOT EXISTS ${legacyTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    assertTrue(syncer.getTargetMeta("${legacyTableName}"))

    sql """INSERT INTO ${legacyTableName} VALUES (1, 10), (2, 20)"""
    assertTrue(syncer.getBinlog("${legacyTableName}"))
    assertTrue(syncer.beginTxn("${legacyTableName}"))
    assertTrue(syncer.getBackendClients())
    assertTrue(syncer.ingestBinlog())
    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT * FROM ${legacyTableName} ORDER BY k"""
    assertEquals(res.size(), 2)
    assertEquals(res[0][0], 1)
    assertEquals(res[0][1], 10)
    assertEquals(res[1][0], 2)
    assertEquals(res[1][1], 20)

    logger.info("=== Test 6: Follower failure and retry with kAlreadyExist ===")
    def retryTableName = "tbl_single_replica_ingest_binlog_follower_retry"
    sql "DROP TABLE IF EXISTS ${retryTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${retryTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    sql """ALTER TABLE ${retryTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${retryTableName}"
    target_sql """
        CREATE TABLE IF NOT EXISTS ${retryTableName}
        (
            `k` INT,
            `v` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 3"
        )
    """
    assertTrue(syncer.getTargetMeta("${retryTableName}"))

    sql """INSERT INTO ${retryTableName} VALUES (1, 10), (2, 20)"""
    assertTrue(syncer.getBinlog("${retryTableName}"))
    assertTrue(syncer.beginTxn("${retryTableName}"))
    assertTrue(syncer.getBackendClients())

    // Pick the leader backend (where the debug point is evaluated) and a follower to fail.
    def tarTableMeta = syncer.context.targetTableMap.get(retryTableName)
    def tarPartitionMeta = tarTableMeta.partitionMap.values().iterator().next()
    def tarTabletEntry = tarPartitionMeta.tabletMeta.entrySet().iterator().next()
    def tabletId = tarTabletEntry.key
    def tarTabletMeta = tarTabletEntry.value
    def leaderIdx = (int) (tabletId % tarTabletMeta.replicas.size())
    def leaderBackendId = -1L
    def followerBackendId = -1L
    def successFollowerBackendId = -1L
    tarTabletMeta.replicas.eachWithIndex { entry, idx ->
        if (idx == leaderIdx) {
            leaderBackendId = entry.value
        } else if (followerBackendId == -1L) {
            followerBackendId = entry.value
        } else {
            successFollowerBackendId = entry.value
        }
    }
    assertTrue(leaderBackendId != -1L, "should find leader replica")
    assertTrue(followerBackendId != -1L, "should find a follower replica to fail")
    assertTrue(successFollowerBackendId != -1L, "should find a follower replica that succeeds")

    def leaderClient = syncer.context.targetBackendClients.get(leaderBackendId)
    assertTrue(leaderClient != null, "should find leader backend client")

    def followerClient = syncer.context.targetBackendClients.get(followerBackendId)
    assertTrue(followerClient != null, "should find follower backend client")

    def successFollowerClient = syncer.context.targetBackendClients.get(successFollowerBackendId)
    assertTrue(successFollowerClient != null, "should find success follower backend client")

    def readMetric = { host, httpPort, metricName ->
        def url = "http://${host}:${httpPort}/metrics"
        def text = Http.GET(url, false, false)
        def m = text =~ /(?m)^${metricName}\s+(\d+)$/
        return m ? Long.parseLong(m[0][1]) : -1L
    }
    def metricName = "doris_be_binlog_ingest_redundant_rowset_cleanup_success_total"

    // The debug point runs in the leader's distribution path and forces the
    // specified follower backend id to fail. enable_debug_points is a static
    // (non-dynamic) BE config, so the cluster must be started with
    // enable_debug_points=true in be.conf; the test relies on that startup
    // configuration rather than trying to toggle it at runtime.
    try {
        DebugPoint.enableDebugPoint(leaderClient.address.hostname, leaderClient.httpPort,
                NodeType.BE, "ingest_binlog.follower.force_fail",
                ["backend_id": String.valueOf(followerBackendId)])

        // First ingest should fail because the follower is forced to fail.
        def firstIngestOk = syncer.ingestBinlogSingleReplica()
        assert !firstIngestOk : "first ingest should fail when follower is forced down"
    } finally {
        DebugPoint.disableDebugPoint(leaderClient.address.hostname, leaderClient.httpPort,
                NodeType.BE, "ingest_binlog.follower.force_fail")
    }

    // Retry: the leader already committed the rowset, so this attempt should hit
    // kAlreadyExist and still fan out to followers. The follower that was forced to fail
    // in the first attempt has no committed rowset, so it will not hit kAlreadyExist.
    // The follower that succeeded in the first attempt will hit kAlreadyExist and delete
    // redundant peer files, so we check its metric.
    def leaderMetricBefore = readMetric(leaderClient.address.hostname, leaderClient.httpPort, metricName)
    def successFollowerMetricBefore = readMetric(successFollowerClient.address.hostname, successFollowerClient.httpPort, metricName)
    logger.info("redundant files deleted metric before retry: leader=${leaderMetricBefore}, successFollower=${successFollowerMetricBefore}")
    assertTrue(leaderMetricBefore != -1, "leader metric ${metricName} should exist")
    assertTrue(successFollowerMetricBefore != -1, "success follower metric ${metricName} should exist")

    assertTrue(syncer.ingestBinlogSingleReplica())

    def leaderMetricAfter = readMetric(leaderClient.address.hostname, leaderClient.httpPort, metricName)
    def successFollowerMetricAfter = readMetric(successFollowerClient.address.hostname, successFollowerClient.httpPort, metricName)
    logger.info("redundant files deleted metric after retry: leader=${leaderMetricAfter}, successFollower=${successFollowerMetricAfter}")
    assertTrue(leaderMetricAfter > leaderMetricBefore,
            "leader should delete redundant rowset files after kAlreadyExist retry")
    assertTrue(successFollowerMetricAfter > successFollowerMetricBefore,
            "follower that succeeded first should delete redundant peer files after kAlreadyExist retry")

    assertTrue(syncer.commitTxn())
    assertTrue(syncer.checkTargetVersion())
    syncer.closeBackendClients()

    target_sql "sync"
    res = target_sql """SELECT * FROM ${retryTableName} ORDER BY k"""
    assertEquals(res.size(), 2)
    assertEquals(res[0][0], 1)
    assertEquals(res[0][1], 10)
    assertEquals(res[1][0], 2)
    assertEquals(res[1][1], 20)
}
