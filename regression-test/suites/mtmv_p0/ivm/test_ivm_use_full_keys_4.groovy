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

suite("test_ivm_use_full_keys_4", "nonConcurrent") {
    String debugPoint = "IvmUtil.buildRowIdHash.force_collision"
    GetDebugPoint().disableDebugPointForAllFEs(debugPoint)

    // Returns the MV key column names (including hidden columns) in physical schema order.
    def getMvKeyColumns = { String mvName ->
        sql """SET show_hidden_columns = true;"""
        def desc = sql """DESC ${mvName}"""
        sql """SET show_hidden_columns = false;"""
        return desc.findAll { row -> row[3].toString() == "true" }
                .collect { row -> row[0].toString() }
    }

    // =========================================================
    // Collision Case B — agg MV
    //   Debug point forces every row_id to 1. The MV keys are
    //   (k1, k2, row_id): initial load keeps both groups, and the
    //   incremental apply join matches on identity keys (null-safe
    //   equality) so group deltas never conflate.
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk4_collision_mv;"""
    sql """drop table if exists test_ivm_uk4_collision_base;"""

    sql """
        CREATE TABLE test_ivm_uk4_collision_base (
            k1 INT,
            k2 INT,
            v1 INT
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_uk4_collision_base VALUES
            (1, 1, 10),
            (2, 2, 20);
    """

    try {
        // Must be enabled before the first refresh so the stored row_id is also 1.
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk4_collision_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true'
            )
            AS SELECT k1, k2, SUM(v1) AS sum_v1
               FROM test_ivm_uk4_collision_base
               GROUP BY k1, k2;
        """

        // Group-by keys + row_id become the MV keys.
        assertEquals(["k1", "k2", "__DORIS_IVM_ROW_ID_COL__"],
                getMvKeyColumns("test_ivm_uk4_collision_mv"))

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk4_collision_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk4_collision_mv")

        // Both groups survive with row_id=1 because (k1, k2, row_id) is the unique key.
        order_qt_uk4_collision_initial """SELECT k1, k2, sum_v1 FROM test_ivm_uk4_collision_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk4_collision_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk4_collision_mv")

        order_qt_uk4_collision_initial_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk4_collision_mv"""

        // MOW base replace: (1,1,10) -> (1,1,5), so group (1,1) delta = -5; (3,3,30) is a new group.
        // The apply join on identity keys merges the (1,1) delta into the right MV row only.
        sql """INSERT INTO test_ivm_uk4_collision_base VALUES (1, 1, 5), (3, 3, 30);"""

        // The incremental apply join must match on the identity keys (null-safe equality)
        // in addition to row_id, otherwise row_id collisions conflate distinct groups.
        def incrPlan = sql """EXPLAIN ANALYZED PLAN REFRESH MATERIALIZED VIEW test_ivm_uk4_collision_mv INCREMENTAL"""
        assertTrue(incrPlan.toString().contains("(k1#"), "apply join should reference identity key k1: " + incrPlan)
        assertTrue(incrPlan.toString().contains("<=>"), "apply join should use null-safe equality: " + incrPlan)

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk4_collision_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk4_collision_mv")

        order_qt_uk4_collision_incremental """SELECT k1, k2, sum_v1 FROM test_ivm_uk4_collision_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk4_collision_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk4_collision_mv")

        order_qt_uk4_collision_incremental_complete """SELECT k1, k2, sum_v1 FROM test_ivm_uk4_collision_mv"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
}
