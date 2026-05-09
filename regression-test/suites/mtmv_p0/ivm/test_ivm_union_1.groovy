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

suite("test_ivm_union_1") {

    // =========================================================
    // Part 1: Basic UNION ALL (MOW + MOW) — insert into both arms
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_1_basic_mv;"""
    sql """drop table if exists test_ivm_union_1_basic_t1;"""
    sql """drop table if exists test_ivm_union_1_basic_t2;"""

    sql """
        CREATE TABLE test_ivm_union_1_basic_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_1_basic_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_1_basic_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_1_basic_t2 VALUES
            (3, 30),
            (4, 40);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_1_basic_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_1_basic_t1
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_1_basic_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_basic_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_basic_mv")
    order_qt_union_basic_after_complete """
        SELECT k1, v1 FROM test_ivm_union_1_basic_mv
    """

    sql """INSERT INTO test_ivm_union_1_basic_t1 VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_basic_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_basic_mv")
    order_qt_union_basic_after_inc1 """
        SELECT k1, v1 FROM test_ivm_union_1_basic_mv
    """

    sql """INSERT INTO test_ivm_union_1_basic_t2 VALUES (6, 60);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_basic_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_basic_mv")
    order_qt_union_basic_after_inc2 """
        SELECT k1, v1 FROM test_ivm_union_1_basic_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_basic_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_basic_mv")
    order_qt_union_basic_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_1_basic_mv
    """

    // =========================================================
    // Part 2: 3-way UNION ALL (MOW + MOW + MOW)
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_1_3way_mv;"""
    sql """drop table if exists test_ivm_union_1_3way_t1;"""
    sql """drop table if exists test_ivm_union_1_3way_t2;"""
    sql """drop table if exists test_ivm_union_1_3way_t3;"""

    sql """
        CREATE TABLE test_ivm_union_1_3way_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_1_3way_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_1_3way_t3 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_union_1_3way_t1 VALUES (1, 10);"""
    sql """INSERT INTO test_ivm_union_1_3way_t2 VALUES (2, 20);"""
    sql """INSERT INTO test_ivm_union_1_3way_t3 VALUES (3, 30);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_1_3way_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_1_3way_t1
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_1_3way_t2
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_1_3way_t3;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_3way_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_3way_mv")
    order_qt_union_3way_after_complete """
        SELECT k1, v1 FROM test_ivm_union_1_3way_mv
    """

    sql """INSERT INTO test_ivm_union_1_3way_t2 VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_3way_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_3way_mv")
    order_qt_union_3way_after_inc """
        SELECT k1, v1 FROM test_ivm_union_1_3way_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_3way_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_3way_mv")
    order_qt_union_3way_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_1_3way_mv
    """

    // =========================================================
    // Part 3: Self-union (same MOW table twice)
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_1_self_mv;"""
    sql """drop table if exists test_ivm_union_1_self_t;"""

    sql """
        CREATE TABLE test_ivm_union_1_self_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_1_self_t VALUES
            (1, 10),
            (2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_1_self_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_1_self_t
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_1_self_t;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_self_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_self_mv")
    order_qt_union_self_after_complete """
        SELECT k1, v1 FROM test_ivm_union_1_self_mv
    """

    sql """INSERT INTO test_ivm_union_1_self_t VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_self_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_self_mv")
    order_qt_union_self_after_inc """
        SELECT k1, v1 FROM test_ivm_union_1_self_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_self_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_self_mv")
    order_qt_union_self_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_1_self_mv
    """

    // =========================================================
    // Part 4: UNION ALL with filter above
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_1_filter_mv;"""
    sql """drop table if exists test_ivm_union_1_filter_t1;"""
    sql """drop table if exists test_ivm_union_1_filter_t2;"""

    sql """
        CREATE TABLE test_ivm_union_1_filter_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_1_filter_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_1_filter_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_1_filter_t2 VALUES
            (1, 100),
            (3, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_1_filter_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_1_filter_t1 WHERE v1 > 15
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_1_filter_t2 WHERE v1 > 15;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_filter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_filter_mv")
    order_qt_union_filter_after_complete """
        SELECT k1, v1 FROM test_ivm_union_1_filter_mv
    """

    sql """INSERT INTO test_ivm_union_1_filter_t1 VALUES (4, 40);"""
    sql """INSERT INTO test_ivm_union_1_filter_t2 VALUES (5, 5);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_filter_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_filter_mv")
    order_qt_union_filter_after_inc """
        SELECT k1, v1 FROM test_ivm_union_1_filter_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_filter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_filter_mv")
    order_qt_union_filter_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_1_filter_mv
    """

    // =========================================================
    // Part 5: UNION ALL with expression in outer project
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_1_expr_mv;"""
    sql """drop table if exists test_ivm_union_1_expr_t1;"""
    sql """drop table if exists test_ivm_union_1_expr_t2;"""

    sql """
        CREATE TABLE test_ivm_union_1_expr_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_union_1_expr_t2 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_union_1_expr_t1 VALUES (1, 10);"""
    sql """INSERT INTO test_ivm_union_1_expr_t2 VALUES (2, 20);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_1_expr_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 * 2 as doubled FROM test_ivm_union_1_expr_t1
        UNION ALL
        SELECT k1, v1 * 2 as doubled FROM test_ivm_union_1_expr_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_expr_mv")
    order_qt_union_expr_after_complete """
        SELECT k1, doubled FROM test_ivm_union_1_expr_mv
    """

    sql """INSERT INTO test_ivm_union_1_expr_t1 VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_expr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_expr_mv")
    order_qt_union_expr_after_inc """
        SELECT k1, doubled FROM test_ivm_union_1_expr_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_1_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_1_expr_mv")
    order_qt_union_expr_after_complete_recovery """
        SELECT k1, doubled FROM test_ivm_union_1_expr_mv
    """
}
