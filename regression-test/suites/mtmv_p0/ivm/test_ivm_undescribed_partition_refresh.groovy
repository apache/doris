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

/**
 * An IVM COMPLETE rebuild replaces every MV partition from the batches of its mapping, so a batch that maps
 * a partition to no base partition writes no rows over the rows the partition holds. A partition the
 * alignment has not caught up with looks exactly like that: a base ADD PARTITION widens a LIST component,
 * the alignment replaces the MV partition it widens with an empty one of the wider keys, and until that
 * replacement lands the partition's own descriptor is one no base partition produces. The task has to
 * refuse such a batch and let the next attempt realign; rebuilding it empty would lose the rows the
 * partition held.
 *
 * <p>The sync below is skipped, which is what leaves the partition behind: it stands in for the alignment
 * not having run yet rather than relying on a race with a concurrent ADD.
 */
suite("test_ivm_undescribed_partition_refresh", "nonConcurrent") {
    def skipPartitionSyncDebugPoint = "MTMVTask.syncPartitionsIfNeeded.skip"
    def skipPartitionSyncFilterDebugPoint = "MTMVTask.syncPartitionsIfNeeded.skip.filter"
    GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncFilterDebugPoint)
    GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncDebugPoint)

    // The MV is checked by reading it, not through a rewrite of the base query.
    sql "SET enable_materialized_view_rewrite = false"

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_undescribed_mv"""
    sql """DROP TABLE IF EXISTS ivm_undescribed_base"""

    sql """
        CREATE TABLE ivm_undescribed_base (
            id INT NOT NULL,
            d DATE NOT NULL,
            k INT NOT NULL,
            v INT
        )
        UNIQUE KEY(id, d, k)
        PARTITION BY LIST(d, k) (
            PARTITION p_first VALUES IN (("2026-01-01", 1)),
            PARTITION p_other VALUES IN (("2026-03-01", 1))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_undescribed_base VALUES (1, "2026-01-01", 1, 10), (2, "2026-03-01", 1, 20)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_undescribed_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(id, d, k)
        PARTITION BY(d)
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, d, k, v FROM ivm_undescribed_base
    """
    sql """REFRESH MATERIALIZED VIEW ivm_undescribed_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_undescribed_mv")
    order_qt_initial """SELECT id, d, k, v FROM ivm_undescribed_mv"""

    // The new partition shares (2026-01-01, 1)'s d and brings 2026-02-01 with it, so the component the MV
    // partition for 2026-01-01 was built from is widened to {2026-01-01, 2026-02-01}. The alignment replaces
    // the partition with an empty one of those keys; until it runs, the partition's own descriptor is one no
    // base partition produces.
    sql """ALTER TABLE ivm_undescribed_base
        ADD PARTITION p_widened VALUES IN (("2026-01-01", 2), ("2026-02-01", 2))"""
    sql """INSERT INTO ivm_undescribed_base VALUES
        (3, "2026-01-01", 2, 30), (4, "2026-02-01", 2, 40)"""

    List<Object> failedTask
    try {
        GetDebugPoint().enableDebugPointForAllFEs(
                skipPartitionSyncFilterDebugPoint, [mv_name: "ivm_undescribed_mv"])
        GetDebugPoint().enableDebugPointForAllFEs(skipPartitionSyncDebugPoint, [execute: 1])
        sql """REFRESH MATERIALIZED VIEW ivm_undescribed_mv COMPLETE"""
        failedTask = waitMTMVTaskTerminal("""
            SELECT TaskId, Status, ErrorMsg FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_undescribed_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """, "test_ivm_undescribed_partition_refresh")
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncFilterDebugPoint)
        GetDebugPoint().disableDebugPointForAllFEs(skipPartitionSyncDebugPoint)
    }
    assertEquals("FAILED", String.valueOf(failedTask.get(1)))
    assertTrue(String.valueOf(failedTask.get(2)).contains("is described by no base partition"))
    // The refused batch wrote nothing: the partition still holds the rows it had.
    order_qt_after_failed """SELECT id, d, k, v FROM ivm_undescribed_mv"""

    // With the alignment running again the partition is replaced by the widened one and the rebuild reads
    // every row of the widened component, the two written after the previous refresh included.
    sql """REFRESH MATERIALIZED VIEW ivm_undescribed_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_undescribed_mv")
    order_qt_after_realign """SELECT id, d, k, v FROM ivm_undescribed_mv"""
}
