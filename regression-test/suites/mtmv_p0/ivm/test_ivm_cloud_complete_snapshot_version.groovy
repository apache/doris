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

suite("test_ivm_cloud_complete_snapshot_version", "nonConcurrent") {
    // This test holds the Cloud FE version cache behind Meta Service.
    if (!isCloudMode()) {
        return
    }

    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    // Disable syncing before creating the source table so a pending sync cannot update its cache.
    setFeConfigTemporary([cloud_enable_version_syncer: false]) {
        sql "SET enable_sql_cache = false"

        sql "DROP MATERIALIZED VIEW IF EXISTS ivm_cloud_complete_snapshot_mv"
        sql "DROP TABLE IF EXISTS ivm_cloud_complete_snapshot_base"
        sql """
            CREATE TABLE ivm_cloud_complete_snapshot_base (k INT NOT NULL, v INT)
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
            CREATE MATERIALIZED VIEW ivm_cloud_complete_snapshot_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS SELECT k, v FROM ivm_cloud_complete_snapshot_base
        """

        def partitionVersion = {
            sql_return_maparray("SHOW PARTITIONS FROM ivm_cloud_complete_snapshot_base").first().VisibleVersion
        }
        def consumption = """
            SELECT LAG = '0' AS caught_up, LAST_CONSUMPTION_TIME > 0 AS consumed
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}' AND UNIT = 'ivm_cloud_complete_snapshot_base'
        """
        // Prime the FE's version-1 cache while the partition is still empty.
        order_qt_empty_partition_version "SELECT ${partitionVersion()}"

        def skipCacheUpdate = "FE.CloudGlobalTransactionMgr.updateVersion.disabled"
        try {
            GetDebugPoint().enableDebugPointForAllFEs(skipCacheUpdate)
            sql "INSERT INTO ivm_cloud_complete_snapshot_base VALUES (7, 100)"
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(skipCacheUpdate)
        }

        order_qt_stale_version_before_refresh "SELECT ${partitionVersion()}"
        // In Cloud mode the consumption view checks Meta Service without updating the FE cache.
        order_qt_pending_stream consumption

        sql "REFRESH MATERIALIZED VIEW ivm_cloud_complete_snapshot_mv COMPLETE"
        waitingMTMVTaskFinishedByMvName("ivm_cloud_complete_snapshot_mv")

        order_qt_complete_rows "SELECT k, v FROM ivm_cloud_complete_snapshot_mv"
        order_qt_consumed_stream consumption
        order_qt_stale_version_after_refresh "SELECT ${partitionVersion()}"

        sql "SET cloud_partition_version_cache_ttl_ms = 0"
        order_qt_ms_version "SELECT ${partitionVersion()}"
    }
}
