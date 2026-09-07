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

suite("test_ivm_inner_join_2") {

    // =========================================================
    // Part 6: Join with expression in project
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_2_expr_mv;"""
    sql """drop table if exists test_ivm_inner_join_2_expr_t1;"""
    sql """drop table if exists test_ivm_inner_join_2_expr_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_2_expr_t1 (
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
        CREATE TABLE test_ivm_inner_join_2_expr_t2 (
            k1 INT,
            v2 INT
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

    sql """INSERT INTO test_ivm_inner_join_2_expr_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_inner_join_2_expr_t2 VALUES (1, 100), (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_2_expr_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_2_expr_t1.k1 AS k1,
            test_ivm_inner_join_2_expr_t1.v1 + test_ivm_inner_join_2_expr_t2.v2 AS sum_v
        FROM test_ivm_inner_join_2_expr_t1
        INNER JOIN test_ivm_inner_join_2_expr_t2
            ON test_ivm_inner_join_2_expr_t1.k1 = test_ivm_inner_join_2_expr_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_expr_mv")
    order_qt_expr_join_after_complete """
        SELECT k1, sum_v FROM test_ivm_inner_join_2_expr_mv
    """

    sql """INSERT INTO test_ivm_inner_join_2_expr_t2 VALUES (2, 220);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_expr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_expr_mv")
    order_qt_expr_join_after_incremental """
        SELECT k1, sum_v FROM test_ivm_inner_join_2_expr_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_expr_mv")
    order_qt_expr_join_after_complete_recovery """
        SELECT k1, sum_v FROM test_ivm_inner_join_2_expr_mv
    """

    // =========================================================
    // Part 7: Join with filter above join
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_2_filter_mv;"""
    sql """drop table if exists test_ivm_inner_join_2_filter_t1;"""
    sql """drop table if exists test_ivm_inner_join_2_filter_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_2_filter_t1 (
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
        CREATE TABLE test_ivm_inner_join_2_filter_t2 (
            k1 INT,
            v2 INT
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
        INSERT INTO test_ivm_inner_join_2_filter_t1 VALUES
            (1, 5),
            (2, 15),
            (3, 25);
    """
    sql """
        INSERT INTO test_ivm_inner_join_2_filter_t2 VALUES
            (1, 100),
            (2, 200),
            (3, 300),
            (4, 400);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_2_filter_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_2_filter_t1.k1 AS k1,
            test_ivm_inner_join_2_filter_t1.v1 AS left_v1,
            test_ivm_inner_join_2_filter_t2.v2 AS right_v2
        FROM test_ivm_inner_join_2_filter_t1
        INNER JOIN test_ivm_inner_join_2_filter_t2
            ON test_ivm_inner_join_2_filter_t1.k1 = test_ivm_inner_join_2_filter_t2.k1
        WHERE test_ivm_inner_join_2_filter_t1.v1 > 10;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_filter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_filter_mv")
    order_qt_filter_join_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_filter_mv
    """

    sql """INSERT INTO test_ivm_inner_join_2_filter_t1 VALUES (4, 35);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_filter_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_filter_mv")
    order_qt_filter_join_after_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_filter_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_filter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_filter_mv")
    order_qt_filter_join_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_filter_mv
    """

    // =========================================================
    // Part 8: MOW x MOW with binlog_op on one side — delete removes joined rows
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_2_op_mv;"""
    sql """drop table if exists test_ivm_inner_join_2_op_t1;"""
    sql """drop table if exists test_ivm_inner_join_2_op_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_2_op_t1 (
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
        CREATE TABLE test_ivm_inner_join_2_op_t2 (
            k1 INT,
            v2 INT
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
        INSERT INTO test_ivm_inner_join_2_op_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_inner_join_2_op_t2 VALUES
            (1, 100),
            (2, 200),
            (3, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_2_op_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_2_op_t1.k1 AS k1,
            test_ivm_inner_join_2_op_t1.v1 AS left_v1,
            test_ivm_inner_join_2_op_t2.v2 AS right_v2
        FROM test_ivm_inner_join_2_op_t1
        INNER JOIN test_ivm_inner_join_2_op_t2
            ON test_ivm_inner_join_2_op_t1.k1 = test_ivm_inner_join_2_op_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_op_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_op_mv")
    order_qt_op_join_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_op_mv
    """

    sql """
        INSERT INTO test_ivm_inner_join_2_op_t1 VALUES
            (3, 30);
    """
    sql """DELETE FROM test_ivm_inner_join_2_op_t1 WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_op_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_op_mv")
    order_qt_op_join_after_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_op_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_op_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_op_mv")
    order_qt_op_join_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_op_mv
    """

    // =========================================================
    // Part 9: DUP × DUP inner join — insert-only incremental (guard present but won't fire)
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_2_dup_dup_mv;"""
    sql """drop table if exists test_ivm_inner_join_2_dup_t1;"""
    sql """drop table if exists test_ivm_inner_join_2_dup_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_2_dup_t1 (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    sql """
        CREATE TABLE test_ivm_inner_join_2_dup_t2 (
            k1 INT,
            v2 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    sql """
        INSERT INTO test_ivm_inner_join_2_dup_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_inner_join_2_dup_t2 VALUES
            (1, 100),
            (2, 200),
            (3, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_2_dup_dup_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_2_dup_t1.k1 AS k1,
            test_ivm_inner_join_2_dup_t1.v1 AS left_v1,
            test_ivm_inner_join_2_dup_t2.v2 AS right_v2
        FROM test_ivm_inner_join_2_dup_t1
        INNER JOIN test_ivm_inner_join_2_dup_t2
            ON test_ivm_inner_join_2_dup_t1.k1 = test_ivm_inner_join_2_dup_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_dup_dup_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_dup_dup_mv")
    order_qt_dup_dup_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_dup_dup_mv
    """

    sql """INSERT INTO test_ivm_inner_join_2_dup_t1 VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_dup_dup_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_dup_dup_mv")
    order_qt_dup_dup_after_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_dup_dup_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_dup_dup_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_dup_dup_mv")
    order_qt_dup_dup_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_2_dup_dup_mv
    """

    // =========================================================
    // Part 10: NULL handling in join key — NULLs must not match in inner join
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_2_null_mv;"""
    sql """drop table if exists test_ivm_inner_join_2_null_t1;"""
    sql """drop table if exists test_ivm_inner_join_2_null_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_2_null_t1 (
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
        CREATE TABLE test_ivm_inner_join_2_null_t2 (
            k1 INT,
            v2 INT
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
        INSERT INTO test_ivm_inner_join_2_null_t1 VALUES
            (1, 10),
            (NULL, 99);
    """
    sql """
        INSERT INTO test_ivm_inner_join_2_null_t2 VALUES
            (1, 100),
            (NULL, 999);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_2_null_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_2_null_t1.k1 AS left_k1,
            test_ivm_inner_join_2_null_t1.v1 AS left_v1,
            test_ivm_inner_join_2_null_t2.v2 AS right_v2
        FROM test_ivm_inner_join_2_null_t1
        INNER JOIN test_ivm_inner_join_2_null_t2
            ON test_ivm_inner_join_2_null_t1.k1 = test_ivm_inner_join_2_null_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_null_mv")
    order_qt_null_join_after_complete """
        SELECT left_k1, left_v1, right_v2 FROM test_ivm_inner_join_2_null_mv
    """

    sql """INSERT INTO test_ivm_inner_join_2_null_t1 VALUES (2, 20);"""
    sql """INSERT INTO test_ivm_inner_join_2_null_t2 VALUES (2, 200);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_null_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_null_mv")
    order_qt_null_join_after_incremental """
        SELECT left_k1, left_v1, right_v2 FROM test_ivm_inner_join_2_null_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_2_null_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_2_null_mv")
    order_qt_null_join_after_complete_recovery """
        SELECT left_k1, left_v1, right_v2 FROM test_ivm_inner_join_2_null_mv
    """
}
