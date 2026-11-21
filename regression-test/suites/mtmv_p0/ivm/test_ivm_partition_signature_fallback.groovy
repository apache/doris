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

suite("test_ivm_partition_signature_fallback", "nonConcurrent") {
    def mvName = "ivm_part_sig_mv"
    def signatureSaltDebugPoint = "IvmPlanSignatureGenerator.generate.signature_salt"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_part_sig_mv"
    sql "DROP TABLE IF EXISTS ivm_part_sig_t"
    sql """
        CREATE TABLE ivm_part_sig_t (
            dt DATE NOT NULL,
            id INT NOT NULL,
            v INT
        )
        UNIQUE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'changelog.enable' = 'true',
            'changelog.need_historical_value' = 'true'
        )
    """
    sql "INSERT INTO ivm_part_sig_t VALUES ('2026-01-10', 1, 10), ('2026-02-10', 2, 20)"
    sql """
        CREATE MATERIALIZED VIEW ivm_part_sig_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1', 'refresh_partition_num' = '1')
        AS SELECT dt, id, v FROM ivm_part_sig_t
    """
    refreshIvm(mvName, "REFRESH MATERIALIZED VIEW ivm_part_sig_mv COMPLETE")
    assertEquals("SUCCESS", latestIvmTask(mvName).Status.toString())
    sql "INSERT INTO ivm_part_sig_t VALUES ('2026-01-11', 3, 30)"

    def taskMethodQuery = { task ->
        """SELECT Status, IvmRefreshMethod FROM tasks('type' = 'mv')
           WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
             AND TaskId = '${task.TaskId}'
           ORDER BY TaskId"""
    }
    try {
        GetDebugPoint().enableDebugPointForAllFEs(signatureSaltDebugPoint, [value: "partition_layout_changed"])
        refreshIvm(mvName, "REFRESH MATERIALIZED VIEW ivm_part_sig_mv PARTITIONS")
        def strictPartitions = latestIvmTask(mvName)
        assertTrue(strictPartitions.ErrorMsg.toString().contains("IVM layout signature mismatch"))
        qt_strict_partitions taskMethodQuery(strictPartitions)

        // The failed partition refresh left a rebuild requirement, so this reaches the incremental rebuild path.
        refreshIvm(mvName, "REFRESH MATERIALIZED VIEW ivm_part_sig_mv INCREMENTAL")
        def strictIncremental = latestIvmTask(mvName)
        assertTrue(strictIncremental.ErrorMsg.toString().contains("IVM layout signature mismatch"))
        qt_strict_incremental taskMethodQuery(strictIncremental)
        order_qt_rows_after_rejected_refreshes "SELECT dt, id, v FROM ivm_part_sig_mv"

        refreshIvm(mvName, "REFRESH MATERIALIZED VIEW ivm_part_sig_mv PARTITIONS FALLBACK")
        def fallback = latestIvmTask(mvName)
        assertEquals("PLAN_SIGNATURE_MISMATCH", fallback.IvmFallbackReason.toString())
        qt_complete_fallback taskMethodQuery(fallback)
        order_qt_rows_after_complete "SELECT dt, id, v FROM ivm_part_sig_mv"

        // The COMPLETE published the new signature, so a later strict partition refresh can use it.
        sql "INSERT INTO ivm_part_sig_t VALUES ('2026-02-11', 4, 40)"
        refreshIvm(mvName, "REFRESH MATERIALIZED VIEW ivm_part_sig_mv PARTITIONS")
        qt_matching_partitions taskMethodQuery(latestIvmTask(mvName))
        order_qt_rows_after_matching_refresh "SELECT dt, id, v FROM ivm_part_sig_mv"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(signatureSaltDebugPoint)
    }
}
