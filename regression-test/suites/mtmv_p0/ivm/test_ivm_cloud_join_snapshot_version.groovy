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
    // Only Cloud refreshes read partition versions from Meta Service.
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
        WHERE l.v > 0
        GROUP BY l.k
    """
    sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
    order_qt_baseline "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

    // Keep the left partition's FE cache stale until the incremental refresh reads it.
    setFeConfigTemporary([cloud_enable_version_syncer: false]) {
        def skipCacheUpdate = "FE.CloudGlobalTransactionMgr.updateVersion.disabled"
        sql "INSERT INTO ivm_cloud_join_snapshot_right VALUES (7, 200)"
        try {
            GetDebugPoint().enableDebugPointForAllFEs(skipCacheUpdate)
            sql "INSERT INTO ivm_cloud_join_snapshot_left VALUES (7, 100)"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(skipCacheUpdate)
        }
        order_qt_stale_left_partition "SELECT k FROM ivm_cloud_join_snapshot_left"
        order_qt_right_partition "SELECT k FROM ivm_cloud_join_snapshot_right"

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv INCREMENTAL"
        waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
        order_qt_first_refresh "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv INCREMENTAL"
        waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
        order_qt_second_refresh "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"

        // A right-only delta pairs with a filtered left post snapshot. CSE must see the source table's MOW metadata.
        setGlobalVarTemporary([experimental_enable_virtual_slot_for_cse: true]) {
            sql "INSERT INTO ivm_cloud_join_snapshot_right VALUES (1, 21)"
            sql "REFRESH MATERIALIZED VIEW ivm_cloud_join_snapshot_mv INCREMENTAL"
            waitingMTMVTaskFinishedByMvName("ivm_cloud_join_snapshot_mv")
            order_qt_right_only_refresh "SELECT k, cnt, total FROM ivm_cloud_join_snapshot_mv"
        }

        sql "SET cloud_partition_version_cache_ttl_ms = 0"
        order_qt_full_join """
            SELECT l.k, COUNT(*), SUM(l.v + r.v)
            FROM ivm_cloud_join_snapshot_left l JOIN ivm_cloud_join_snapshot_right r ON l.k = r.k
            WHERE l.v > 0
            GROUP BY l.k
        """
    }
}
