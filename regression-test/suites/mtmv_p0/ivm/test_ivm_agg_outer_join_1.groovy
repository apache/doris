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

suite("test_ivm_agg_outer_join_1") {

    // =========================================================
    // O2 root aggregate above a LEFT OUTER JOIN chain.
    // =========================================================
    sql """drop materialized view if exists test_ivm_agg_outer_join_1_chain_mv;"""
    sql """drop table if exists test_ivm_agg_outer_join_1_chain_a;"""
    sql """drop table if exists test_ivm_agg_outer_join_1_chain_b;"""
    sql """drop table if exists test_ivm_agg_outer_join_1_chain_c;"""

    sql """
        CREATE TABLE test_ivm_agg_outer_join_1_chain_a (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_agg_outer_join_1_chain_b (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_agg_outer_join_1_chain_c (
            k1 INT,
            v3 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO test_ivm_agg_outer_join_1_chain_a VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO test_ivm_agg_outer_join_1_chain_b VALUES (1, 100), (3, 300);"""
    sql """INSERT INTO test_ivm_agg_outer_join_1_chain_c VALUES (1, 1000);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_outer_join_1_chain_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            test_ivm_agg_outer_join_1_chain_a.k1 AS k1,
            COUNT(*) AS row_count,
            COUNT(test_ivm_agg_outer_join_1_chain_b.v2) AS b_count,
            SUM(test_ivm_agg_outer_join_1_chain_c.v3) AS c_sum
        FROM test_ivm_agg_outer_join_1_chain_a
        LEFT OUTER JOIN test_ivm_agg_outer_join_1_chain_b
            ON test_ivm_agg_outer_join_1_chain_a.k1 = test_ivm_agg_outer_join_1_chain_b.k1
        LEFT OUTER JOIN test_ivm_agg_outer_join_1_chain_c
            ON test_ivm_agg_outer_join_1_chain_a.k1 = test_ivm_agg_outer_join_1_chain_c.k1
        GROUP BY test_ivm_agg_outer_join_1_chain_a.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_outer_join_1_chain_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_outer_join_1_chain_mv")
    order_qt_chain_agg_after_complete """
        SELECT k1, row_count, b_count, c_sum
        FROM test_ivm_agg_outer_join_1_chain_mv
        ORDER BY k1
    """

    sql """INSERT INTO test_ivm_agg_outer_join_1_chain_a VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_outer_join_1_chain_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_outer_join_1_chain_mv")
    order_qt_chain_agg_after_left_incremental """
        SELECT k1, row_count, b_count, c_sum
        FROM test_ivm_agg_outer_join_1_chain_mv
        ORDER BY k1
    """

    sql """INSERT INTO test_ivm_agg_outer_join_1_chain_c VALUES (3, 3000);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_outer_join_1_chain_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_outer_join_1_chain_mv")
    order_qt_chain_agg_after_right_incremental """
        SELECT k1, row_count, b_count, c_sum
        FROM test_ivm_agg_outer_join_1_chain_mv
        ORDER BY k1
    """
}
