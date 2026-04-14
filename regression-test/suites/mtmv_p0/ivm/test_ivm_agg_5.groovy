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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_agg_5") {

    // =========================================================
    // Part 1: Bare GROUP BY without aggregate functions (SELECT DISTINCT)
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_bare_grpby_mv;"""
    sql """drop table if exists test_ivm_agg_bare_grpby_base;"""

    sql """
        CREATE TABLE test_ivm_agg_bare_grpby_base (
            k1 INT,
            k2 VARCHAR(32),
            v1 INT,
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_bare_grpby_base VALUES
            (1, 'a', 10, 0),
            (2, 'b', 20, 0),
            (3, 'a', 30, 0),
            (4, 'b', 40, 0);
    """

    // Create IVM MV with bare GROUP BY — equivalent to SELECT DISTINCT k2
    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k2 FROM test_ivm_agg_bare_grpby_base GROUP BY k2;
    """

    // Verify MV created successfully
    def mvInfos = sql """select State from mv_infos('database'='${context.dbName}') where Name = 'test_ivm_agg_bare_grpby_mv'"""
    assertTrue(mvInfos.toString().contains("INIT"))

    // First COMPLETE refresh — should produce distinct k2 values: 'a', 'b'
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    order_qt_bare_grpby_after_complete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Insert new data with a new group value 'c'
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (5, 'c', 50, 0);"""

    // INCREMENTAL refresh then COMPLETE to verify correctness
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    // Should now have: 'a', 'b', 'c'
    order_qt_bare_grpby_after_insert """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Delete one 'a' row (k1=3), but 'a' still exists via k1=1
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (3, 'a', 30, 1);"""
    // Dirty partition
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (6, 'c', 60, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    // Should still have: 'a', 'b', 'c' (k1=1 still has 'a')
    order_qt_bare_grpby_after_delete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // Delete all 'b' rows (k1=2, k1=4) — group 'b' should disappear
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (2, 'b', 20, 1);"""
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (4, 'b', 40, 1);"""
    // Dirty partition
    sql """INSERT INTO test_ivm_agg_bare_grpby_base VALUES (7, 'a', 70, 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_grpby_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_grpby_mv")

    // Should have: 'a', 'c' (group 'b' gone)
    order_qt_bare_grpby_after_group_delete """SELECT k2 FROM test_ivm_agg_bare_grpby_mv"""

    // =========================================================
    // Part 2: Multi-column bare GROUP BY
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_bare_multikey_mv;"""
    sql """drop table if exists test_ivm_agg_bare_multikey_base;"""

    sql """
        CREATE TABLE test_ivm_agg_bare_multikey_base (
            k1 INT,
            k2 INT,
            v1 VARCHAR(32),
            binlog_op TINYINT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_agg_bare_multikey_base VALUES
            (1, 10, 'x', 0),
            (2, 10, 'y', 0),
            (3, 20, 'x', 0),
            (4, 20, 'y', 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k2, v1 FROM test_ivm_agg_bare_multikey_base GROUP BY k2, v1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_multikey_mv")

    // 4 distinct (k2, v1) combos: (10,'x'), (10,'y'), (20,'x'), (20,'y')
    order_qt_bare_multi_after_complete """SELECT k2, v1 FROM test_ivm_agg_bare_multikey_mv"""

    // Insert a new unique combination
    sql """INSERT INTO test_ivm_agg_bare_multikey_base VALUES (5, 30, 'z', 0);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_multikey_mv")

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_bare_multikey_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_bare_multikey_mv")

    // 5 distinct combos now
    order_qt_bare_multi_after_insert """SELECT k2, v1 FROM test_ivm_agg_bare_multikey_mv"""
}
