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

import org.apache.doris.regression.util.Http
import org.awaitility.Awaitility

import java.util.concurrent.TimeUnit

suite("test_row_binlog_ttl_clone", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    def backends = sql_return_maparray("SHOW BACKENDS").findAll { it.Alive.toBoolean() }
    if (backends.size() < 2) {
        logger.info("TTL clone race requires at least two live backends")
        return
    }
    def backendById = backends.collectEntries { [(it.BackendId): it] }
    def readConfig = { replica ->
        def backend = backendById[replica.BackendId]
        Http.GET("http://${backend.Host}:${backend.HttpPort}/api/meta/header/${replica.TabletId}"
                + "?byte_to_base64=true", true, false).binlog_config
    }
    def replicas = {
        sql_return_maparray("SHOW TABLETS FROM row_binlog_ttl_clone")
    }
    def snapshotPoint = "SchemaChangeHandler.updatePartitionProperties.row_binlog_after_replica_snapshot"
    def alterFuture

    // These queries must read physical rowsets from the selected replica after cleanup.
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_clone"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_clone_scanner"
    sql """
        CREATE TABLE row_binlog_ttl_clone (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1", "binlog.enable"="true", "binlog.format"="ROW",
                    "binlog.ttl_seconds"="1", "disable_auto_compaction"="true")
    """
    sql "INSERT INTO row_binlog_ttl_clone VALUES (1, 10), (2, 20)"
    order_qt_before_clone "SELECT k, v FROM binlog('table'='row_binlog_ttl_clone') ORDER BY k, v"
    def originalReplicas = replicas()
    def originalIds = originalReplicas.collect { it.ReplicaId }.toSet()
    def oldGeneration = readConfig(originalReplicas[0]).config_version ?: 0L

    try {
        sql "SET fallback_other_replica_when_fixed_corrupt = false"
        GetDebugPoint().enableDebugPointForAllFEs(snapshotPoint, [timeout: "300"])
        alterFuture = thread("extend-row-binlog-ttl") {
            sql """ALTER TABLE row_binlog_ttl_clone SET ("binlog.ttl_seconds"="86400")"""
        }
        // The marker is installed after the replica snapshot and before any update RPC.
        // Waiting for it makes the new replica absent from the initial ACK set deterministically.
        Awaitility.await().pollInSameThread().atMost(60, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until {
            sql_return_maparray("SHOW FULL PROCESSLIST").any { it.TraceId == snapshotPoint }
        }
        sql """ALTER TABLE row_binlog_ttl_clone
               MODIFY PARTITION (*) SET ("replication_num"="2")"""
        sql "ADMIN REPAIR TABLE row_binlog_ttl_clone"
        Awaitility.await().pollInSameThread().atMost(180, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS).until {
            def current = replicas()
            current.size() == originalReplicas.size() * 2 && current.every { it.State == "NORMAL" }
        }
        def clonedReplicas = replicas().findAll { !originalIds.contains(it.ReplicaId) }
        def sourceConfigs = originalReplicas.collect { readConfig(it) }
        def cloneConfigs = clonedReplicas.collect { readConfig(it) }
        // No update RPC has run: the source .hdr still has TTL=1. A NORMAL clone must
        // already contain the pending target config, installed before scanner registration.
        order_qt_old_snapshot_source """
            SELECT ${sourceConfigs.every { (it.ttl_seconds as long) == 1L }},
                   ${sourceConfigs.every { (it.config_version ?: 0L) == oldGeneration }}
        """
        def targetGeneration = cloneConfigs[0].config_version as long
        order_qt_clone_config_before_publish """
            SELECT ${cloneConfigs.every { (it.ttl_seconds as long) == 86400L }},
                   ${targetGeneration > (oldGeneration as long)},
                   ${cloneConfigs.every { (it.config_version as long) == targetGeneration }}
        """
        GetDebugPoint().disableDebugPointForAllFEs(snapshotPoint)
        alterFuture.get(180, TimeUnit.SECONDS)

        def configs = replicas().collect { readConfig(it) }
        def ddl = sql("SHOW CREATE TABLE row_binlog_ttl_clone")[0][1]
        def publishedTtl = (ddl =~ /"binlog\.ttl_seconds" = "(-?\d+)"/)[0][1]
        order_qt_all_replicas_acknowledged """
            SELECT ${publishedTtl}, ${configs.size()},
                   ${configs.every { (it.ttl_seconds as long) == 86400L }},
                   ${configs.every { (it.config_version as long) == targetGeneration }}
        """
        sql """ALTER TABLE row_binlog_ttl_clone SET ("disable_auto_compaction"="false")"""

        // A short-TTL control replica on every BE proves cleanup is running. Each round
        // starts only after all replicas reclaimed the preceding row; there are no new
        // writes to the clone table that could mask loss or trigger its TTL compaction.
        sql """
            CREATE TABLE row_binlog_ttl_clone_scanner (k INT, v INT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="${backends.size()}", "binlog.enable"="true",
                        "binlog.format"="ROW", "binlog.ttl_seconds"="1")
        """
        for (int round = 1; round <= 3; ++round) {
            sql """ALTER TABLE row_binlog_ttl_clone_scanner SET ("disable_auto_compaction"="true")"""
            sql "INSERT INTO row_binlog_ttl_clone_scanner VALUES (${round}, ${round})"
            def countsBeforeCleanup = (0..<backends.size()).collect { replicaIndex ->
                sql "SET use_fix_replica = ${replicaIndex}"
                sql("SELECT count(*) FROM binlog('table'='row_binlog_ttl_clone_scanner')")[0][0] as long
            }
            order_qt_scanner_row_before_cleanup "SELECT ${countsBeforeCleanup.every { it == 1L }}"
            sql """ALTER TABLE row_binlog_ttl_clone_scanner SET ("disable_auto_compaction"="false")"""
            Awaitility.await().pollInSameThread().atMost(120, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS).until {
                (0..<backends.size()).every { replicaIndex ->
                    sql "SET use_fix_replica = ${replicaIndex}"
                    (sql("SELECT count(*) FROM binlog('table'='row_binlog_ttl_clone_scanner')")[0][0]
                            as long) == 0L
                }
            }
            for (int replicaIndex = 0; replicaIndex < 2; ++replicaIndex) {
                sql "SET use_fix_replica = ${replicaIndex}"
                order_qt_retained_after_scanner_round """
                    SELECT k, v FROM binlog('table'='row_binlog_ttl_clone') ORDER BY k, v
                """
            }
        }
        for (int replicaIndex = 0; replicaIndex < 2; ++replicaIndex) {
            sql "SET use_fix_replica = ${replicaIndex}"
            order_qt_incr_replica """
                SELECT k, v FROM row_binlog_ttl_clone@incr('incrementType'='DETAIL') ORDER BY k, v
            """
            order_qt_base_replica "SELECT k, v FROM row_binlog_ttl_clone ORDER BY k, v"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(snapshotPoint)
        if (alterFuture != null && !alterFuture.isDone()) {
            alterFuture.get(180, TimeUnit.SECONDS)
        }
        sql "SET use_fix_replica = -1"
        sql "SET fallback_other_replica_when_fixed_corrupt = true"
    }
}
