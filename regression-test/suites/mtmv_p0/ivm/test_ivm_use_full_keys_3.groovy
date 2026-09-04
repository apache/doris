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

suite("test_ivm_use_full_keys_3", "nonConcurrent") {
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
    // Collision Case A — linear MOW MV
    //   Debug point forces every row_id to 1. Without full keys the
    //   MOW dedup would collapse rows whose unique key is (row_id) only;
    //   with use_full_keys the MV keys are (base_uk, row_id), so k1
    //   distinct rows survive and deletes only touch the target row.
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk3_linear_mv;"""
    sql """drop table if exists test_ivm_uk3_linear_base;"""

    sql """
        CREATE TABLE test_ivm_uk3_linear_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_uk3_linear_base VALUES
            (1, 10),
            (2, 20);
    """

    try {
        // Must be enabled before the first refresh so the stored row_id is also 1.
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk3_linear_mv
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true'
            )
            AS SELECT k1, v1 FROM test_ivm_uk3_linear_base;
        """

        // Base unique key + row_id become the MV keys.
        assertEquals(["k1", "__DORIS_IVM_ROW_ID_COL__"], getMvKeyColumns("test_ivm_uk3_linear_mv"))

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk3_linear_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk3_linear_mv")

        // Both k1 rows survive despite identical row_id=1.
        order_qt_uk3_linear_collision_initial """SELECT k1, v1 FROM test_ivm_uk3_linear_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk3_linear_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk3_linear_mv")

        order_qt_uk3_linear_collision_initial_complete """SELECT k1, v1 FROM test_ivm_uk3_linear_mv"""

        // Insert a new row and delete k1=1 — the delete must only remove k1=1.
        sql """INSERT INTO test_ivm_uk3_linear_base VALUES (3, 30);"""
        sql """DELETE FROM test_ivm_uk3_linear_base WHERE k1 = 1;"""
        sql """INSERT INTO test_ivm_uk3_linear_base VALUES (4, 40);"""
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk3_linear_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk3_linear_mv")

        order_qt_uk3_linear_collision_after_delete """SELECT k1, v1 FROM test_ivm_uk3_linear_mv"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk3_linear_mv COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk3_linear_mv")

        order_qt_uk3_linear_collision_after_delete_complete """SELECT k1, v1 FROM test_ivm_uk3_linear_mv"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
}
