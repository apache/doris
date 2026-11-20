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

suite("test_ivm_cloud_join_snapshot_version", "nonConcurrent") {
    // This test uses the Cloud transaction manager's version-cache debug point.
    if (!isCloudMode()) {
        return
    }

    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_cloud_join_snapshot_mv"
    sql "DROP TABLE IF EXISTS ivm_cloud_join_snapshot_left"
    sql "DROP TABLE IF EXISTS ivm_cloud_join_snapshot_right"

    sql """
        CREATE TABLE ivm_cloud_join_snapshot_left (k INT NOT NULL, v INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE ivm_cloud_join_snapshot_right (k INT NOT NULL, v INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO ivm_cloud_join_snapshot_left VALUES (1, 10)"
    sql "INSERT INTO ivm_cloud_join_snapshot_right VALUES (1, 20)"

    sql """
        CREATE MATERIALIZED VIEW ivm_cloud_join_snapshot_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT l.k AS k, COUNT(*) AS cnt, SUM(l.v + r.v) AS total
        FROM ivm_cloud_join_snapshot_left l JOIN ivm_cloud_join_snapshot_right r ON l.k = r.k
        GROUP BY l.k
    """
    sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
    order_qt_baseline "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

    def syncer = sql_return_maparray "ADMIN SHOW FRONTEND CONFIG LIKE 'cloud_enable_version_syncer'"
    def oldSyncer = syncer.first().Value
    def skipCacheUpdate = "FE.CloudGlobalTransactionMgr.updateVersion.disabled"
    try {
        sql "ADMIN SET FRONTEND CONFIG ('cloud_enable_version_syncer' = 'false')"
        sql "INSERT INTO ivm_cloud_join_snapshot_right VALUES (7, 200)"
        GetDebugPoint().enableDebugPointForAllFEs(skipCacheUpdate)
        try {
            sql "INSERT INTO ivm_cloud_join_snapshot_left VALUES (7, 100)"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(skipCacheUpdate)
        }
        order_qt_stale_left_partition "SELECT k FROM ivm_cloud_join_snapshot_left"

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv INCREMENTAL"
        waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
        order_qt_first_refresh "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv INCREMENTAL"
        waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
        order_qt_second_refresh "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

        sql "SET cloud_partition_version_cache_ttl_ms = 0"
        order_qt_full_join """
            SELECT l.k, COUNT(*), SUM(l.v + r.v)
            FROM ivm_cloud_join_snapshot_left l JOIN ivm_cloud_join_snapshot_right r ON l.k = r.k
            GROUP BY l.k
        """
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('cloud_enable_version_syncer' = '${oldSyncer}')"
    }
}
