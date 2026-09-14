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

suite("test_ivm_use_full_keys_6", "nonConcurrent") {
    String debugPoint = "IvmUtil.buildRowIdHash.force_collision"
    GetDebugPoint().disableDebugPointForAllFEs(debugPoint)

    // =========================================================
    // Chained collision case: MV3 = MV1 JOIN MV2, all with ivm_use_full_keys.
    //   Debug point forces every row_id to 1. MV1/MV2 keys are (uk, row_id) and
    //   MV3 keys are (mv1 uk, mv2 uk, row_id), so distinct join rows survive the
    //   row_id collisions through both the incremental and complete refreshes.
    // =========================================================

    sql """drop materialized view if exists test_ivm_uk6_mv3;"""
    sql """drop materialized view if exists test_ivm_uk6_mv2;"""
    sql """drop materialized view if exists test_ivm_uk6_mv1;"""
    sql """drop table if exists test_ivm_uk6_t1;"""
    sql """drop table if exists test_ivm_uk6_t2;"""

    sql """
        CREATE TABLE test_ivm_uk6_t1 (
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
        CREATE TABLE test_ivm_uk6_t2 (
            k2 INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_uk6_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_uk6_t2 VALUES (1, 100), (2, 200);"""

    try {
        // Must be enabled before the first refresh so the stored row_ids are also 1.
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint)

        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk6_mv1
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true',
                'binlog.enable' = 'true',
                'binlog.format' = 'ROW',
                'binlog.need_historical_value' = 'true'
            )
            AS SELECT k1, v1 FROM test_ivm_uk6_t1;
        """
        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk6_mv2
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true',
                'binlog.enable' = 'true',
                'binlog.format' = 'ROW',
                'binlog.need_historical_value' = 'true'
            )
            AS SELECT k2, v2 FROM test_ivm_uk6_t2;
        """

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv1 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv1")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv2 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv2")

        order_qt_uk6_mv1_initial """SELECT k1, v1 FROM test_ivm_uk6_mv1"""
        order_qt_uk6_mv2_initial """SELECT k2, v2 FROM test_ivm_uk6_mv2"""

        sql """
            CREATE MATERIALIZED VIEW test_ivm_uk6_mv3
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                'replication_num' = '1',
                'ivm_use_full_keys' = 'true'
            )
            AS SELECT a.k1, b.k2, a.v1, b.v2
               FROM test_ivm_uk6_mv1 a JOIN test_ivm_uk6_mv2 b
               ON a.k1 = b.k2;
        """

        // Dump all three MVs' physical schemas (including hidden columns) into the .out so
        // the chained MV3 key columns, which carry the renamed mv1/mv2 row ids, are visible.
        sql """SET show_hidden_columns = true;"""
        qt_uk6_mv1_desc """DESC test_ivm_uk6_mv1"""
        qt_uk6_mv2_desc """DESC test_ivm_uk6_mv2"""
        qt_uk6_mv3_desc """DESC test_ivm_uk6_mv3"""
        sql """SET show_hidden_columns = false;"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv3 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv3")

        order_qt_uk6_mv3_initial """SELECT k1, k2, v1, v2 FROM test_ivm_uk6_mv3"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv1 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv1")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv2 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv2")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv3 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv3")

        order_qt_uk6_mv1_initial_complete """SELECT k1, v1 FROM test_ivm_uk6_mv1"""
        order_qt_uk6_mv2_initial_complete """SELECT k2, v2 FROM test_ivm_uk6_mv2"""
        order_qt_uk6_mv3_initial_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk6_mv3"""

        // Insert new matching pairs, then refresh the chain bottom-up.
        sql """INSERT INTO test_ivm_uk6_t1 VALUES (3, 30);"""
        sql """INSERT INTO test_ivm_uk6_t2 VALUES (3, 300);"""
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv1 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv1")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv2 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv2")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv3 INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv3")

        order_qt_uk6_mv1_incremental """SELECT k1, v1 FROM test_ivm_uk6_mv1"""
        order_qt_uk6_mv2_incremental """SELECT k2, v2 FROM test_ivm_uk6_mv2"""
        order_qt_uk6_mv3_incremental """SELECT k1, k2, v1, v2 FROM test_ivm_uk6_mv3"""

        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv1 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv1")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv2 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv2")
        sql """REFRESH MATERIALIZED VIEW test_ivm_uk6_mv3 COMPLETE"""
        waitingMTMVTaskFinishedByMvName("test_ivm_uk6_mv3")

        order_qt_uk6_mv1_incremental_complete """SELECT k1, v1 FROM test_ivm_uk6_mv1"""
        order_qt_uk6_mv2_incremental_complete """SELECT k2, v2 FROM test_ivm_uk6_mv2"""
        order_qt_uk6_mv3_incremental_complete """SELECT k1, k2, v1, v2 FROM test_ivm_uk6_mv3"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }
}
