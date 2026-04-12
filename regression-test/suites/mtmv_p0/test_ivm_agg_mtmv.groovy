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

suite("test_ivm_agg_mtmv") {
    sql """drop materialized view if exists test_ivm_agg_mtmv_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_base;"""

    // 1. Create MOW base table
    sql """
        CREATE TABLE test_ivm_agg_mtmv_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // 2. Insert initial rows: groups k1=1,2,3
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // 3. Create IVM MV with grouped aggregate (COUNT(*) + SUM)
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1 FROM test_ivm_agg_mtmv_base GROUP BY k1;
    """

    // 4. Verify MV metadata
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_mv'"""
    assertTrue(mvInfos.toString().contains("INIT"))

    // 5. Verify MV is UNIQUE_KEYS
    def descResult = sql """desc test_ivm_agg_mtmv_mv all"""
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    // 6. First COMPLETE refresh
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // Each k1 maps to 1 row, so COUNT(*)=1, SUM(v1)=v1
    order_qt_agg_after_first_complete """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // 7. Insert new rows: new group k1=4 and update existing k1=1 (MOW upsert replaces v1)
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (4, 40),
            (1, 15);
    """

    // 8. INCREMENTAL refresh — verify it completes without a type-mismatch error.
    //    NOTE: The current IVM mock reads the full base table as delta, so aggregate values
    //    after an INCREMENTAL refresh are not semantically correct (group counts may be inflated).
    //    We only assert that the task finishes in SUCCESS state (no BE crash / type error).
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // 9. Update another row: k1=2 gets new value
    sql """
        INSERT INTO test_ivm_agg_mtmv_base VALUES
            (2, 25);
    """

    // 10. Second INCREMENTAL refresh — also assert no error
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    // 11. COMPLETE refresh — produces correct results (full recomputation)
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_mv")

    order_qt_agg_after_complete """SELECT k1, cnt, sum_v1 FROM test_ivm_agg_mtmv_mv"""

    // =========================================================
    // Part 2: Scalar aggregate MV (no GROUP BY)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_mtmv_scalar_mv;"""
    sql """drop table if exists test_ivm_agg_mtmv_scalar_base;"""

    sql """
        CREATE TABLE test_ivm_agg_mtmv_scalar_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES
            (1, 10),
            (2, 20),
            (3, 30);
    """

    // Scalar agg MV: COUNT(*), SUM, AVG, COUNT(expr) over the whole table
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT COUNT(*) AS total_cnt, SUM(v1) AS total_sum, AVG(v1) AS avg_v1, COUNT(v1) AS cnt_v1
           FROM test_ivm_agg_mtmv_scalar_base;
    """

    def scalarMvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_mtmv_scalar_mv'"""
    assertTrue(scalarMvInfos.toString().contains("INIT"))

    def scalarDescResult = sql """desc test_ivm_agg_mtmv_scalar_mv all"""
    assertTrue(scalarDescResult.toString().contains("UNIQUE_KEYS"))

    // First COMPLETE refresh: 3 rows, total_cnt=3, total_sum=60, avg=20, cnt_v1=3
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_first_complete """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """

    // Upsert k1=1 (v1: 10 → 15)
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (1, 15);"""

    // INCREMENTAL refresh — only asserts SUCCESS (no crash / type-mismatch),
    // since mock IVM reads the full base table so scalar agg values may not be correct.
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    // Insert new row k1=4
    sql """INSERT INTO test_ivm_agg_mtmv_scalar_base VALUES (4, 40);"""

    // Second INCREMENTAL refresh — also assert no error
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    // Final COMPLETE refresh: k1=1(15),2(20),3(30),4(40)
    // total_cnt=4, total_sum=105, avg=26.25, cnt_v1=4
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_mtmv_scalar_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_mtmv_scalar_mv")

    order_qt_scalar_after_final_complete """
        SELECT total_cnt, total_sum, avg_v1, cnt_v1 FROM test_ivm_agg_mtmv_scalar_mv
    """
}
