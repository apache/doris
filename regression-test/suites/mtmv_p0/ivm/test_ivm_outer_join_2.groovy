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

suite("test_ivm_outer_join_2") {

    // =========================================================
    // Part 1: O2 left-deep LEFT OUTER JOIN chain.
    // The lower LEFT JOIN is on the retained side of the upper LEFT JOIN, so this shape is allowed.
    // =========================================================
    sql """drop materialized view if exists ivm_oj2_chain_mv;"""
    sql """drop table if exists ivm_oj2_chain_a;"""
    sql """drop table if exists ivm_oj2_chain_b;"""
    sql """drop table if exists ivm_oj2_chain_c;"""

    sql """
        CREATE TABLE ivm_oj2_chain_a (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj2_chain_b (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj2_chain_c (
            k1 INT,
            v3 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_oj2_chain_a VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO ivm_oj2_chain_b VALUES (1, 100);"""
    sql """INSERT INTO ivm_oj2_chain_c VALUES (1, 1000), (3, 3000);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj2_chain_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_oj2_chain_a.k1 AS k1,
            ivm_oj2_chain_a.v1 AS left_v1,
            ivm_oj2_chain_b.v2 AS mid_v2,
            ivm_oj2_chain_c.v3 AS right_v3
        FROM ivm_oj2_chain_a
        LEFT OUTER JOIN ivm_oj2_chain_b
            ON ivm_oj2_chain_a.k1 = ivm_oj2_chain_b.k1
        LEFT OUTER JOIN ivm_oj2_chain_c
            ON ivm_oj2_chain_a.k1 = ivm_oj2_chain_c.k1;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj2_chain_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_chain_mv")
    advance_ivm_stream_offset("ivm_oj2_chain_mv")
    order_qt_chain_after_complete """
        SELECT k1, left_v1, mid_v2, right_v3
        FROM ivm_oj2_chain_mv
        ORDER BY k1, left_v1, mid_v2, right_v3
    """

    sql """INSERT INTO ivm_oj2_chain_a VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_chain_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_chain_mv")
    order_qt_chain_after_left_incremental """
        SELECT k1, left_v1, mid_v2, right_v3
        FROM ivm_oj2_chain_mv
        ORDER BY k1, left_v1, mid_v2, right_v3
    """

    sql """INSERT INTO ivm_oj2_chain_b VALUES (2, 200);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_chain_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_chain_mv")
    order_qt_chain_after_middle_incremental """
        SELECT k1, left_v1, mid_v2, right_v3
        FROM ivm_oj2_chain_mv
        ORDER BY k1, left_v1, mid_v2, right_v3
    """

    sql """INSERT INTO ivm_oj2_chain_c VALUES (2, 2000);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_chain_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_chain_mv")
    order_qt_chain_after_right_incremental """
        SELECT k1, left_v1, mid_v2, right_v3
        FROM ivm_oj2_chain_mv
        ORDER BY k1, left_v1, mid_v2, right_v3
    """

    // =========================================================
    // Part 2: O2 nested INNER JOIN under LEFT OUTER JOIN.
    // =========================================================
    sql """drop materialized view if exists ivm_oj2_inner_nested_mv;"""
    sql """drop table if exists ivm_oj2_inner_nested_a;"""
    sql """drop table if exists ivm_oj2_inner_nested_b;"""
    sql """drop table if exists ivm_oj2_inner_nested_c;"""

    sql """
        CREATE TABLE ivm_oj2_inner_nested_a (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj2_inner_nested_b (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj2_inner_nested_c (
            k1 INT,
            v3 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_oj2_inner_nested_a VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO ivm_oj2_inner_nested_b VALUES (1, 100), (2, 200);"""
    sql """INSERT INTO ivm_oj2_inner_nested_c VALUES (1, 1000);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj2_inner_nested_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_oj2_inner_nested_a.k1 AS k1,
            ivm_oj2_inner_nested_a.v1 AS left_v1,
            ivm_oj2_inner_nested_b.v2 AS inner_v2,
            ivm_oj2_inner_nested_c.v3 AS right_v3
        FROM ivm_oj2_inner_nested_a
        INNER JOIN ivm_oj2_inner_nested_b
            ON ivm_oj2_inner_nested_a.k1 = ivm_oj2_inner_nested_b.k1
        LEFT OUTER JOIN ivm_oj2_inner_nested_c
            ON ivm_oj2_inner_nested_a.k1 = ivm_oj2_inner_nested_c.k1;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj2_inner_nested_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_inner_nested_mv")
    advance_ivm_stream_offset("ivm_oj2_inner_nested_mv")
    order_qt_inner_nested_after_complete """
        SELECT k1, left_v1, inner_v2, right_v3
        FROM ivm_oj2_inner_nested_mv
        ORDER BY k1, left_v1, inner_v2, right_v3
    """

    sql """INSERT INTO ivm_oj2_inner_nested_b VALUES (3, 300);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_inner_nested_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_inner_nested_mv")
    order_qt_inner_nested_after_inner_incremental """
        SELECT k1, left_v1, inner_v2, right_v3
        FROM ivm_oj2_inner_nested_mv
        ORDER BY k1, left_v1, inner_v2, right_v3
    """

    sql """INSERT INTO ivm_oj2_inner_nested_c VALUES (2, 2000);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_inner_nested_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_inner_nested_mv")
    order_qt_inner_nested_after_right_incremental """
        SELECT k1, left_v1, inner_v2, right_v3
        FROM ivm_oj2_inner_nested_mv
        ORDER BY k1, left_v1, inner_v2, right_v3
    """

    // =========================================================
    // Part 3: O2 filter above LEFT OUTER JOIN.
    // =========================================================
    sql """drop materialized view if exists ivm_oj2_filter_mv;"""
    sql """drop table if exists ivm_oj2_filter_l;"""
    sql """drop table if exists ivm_oj2_filter_r;"""

    sql """
        CREATE TABLE ivm_oj2_filter_l (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj2_filter_r (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_oj2_filter_l VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO ivm_oj2_filter_r VALUES (1, 100), (2, 50);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj2_filter_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_oj2_filter_l.k1 AS k1,
            ivm_oj2_filter_l.v1 AS left_v1,
            ivm_oj2_filter_r.v2 AS right_v2
        FROM ivm_oj2_filter_l
        LEFT OUTER JOIN ivm_oj2_filter_r
            ON ivm_oj2_filter_l.k1 = ivm_oj2_filter_r.k1
        WHERE ivm_oj2_filter_r.v2 IS NULL
              OR ivm_oj2_filter_r.v2 >= 100;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj2_filter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_filter_mv")
    advance_ivm_stream_offset("ivm_oj2_filter_mv")
    order_qt_filter_after_complete """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj2_filter_mv
        ORDER BY k1, left_v1, right_v2
    """

    sql """INSERT INTO ivm_oj2_filter_l VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_filter_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_filter_mv")
    order_qt_filter_after_left_incremental """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj2_filter_mv
        ORDER BY k1, left_v1, right_v2
    """

    sql """INSERT INTO ivm_oj2_filter_r VALUES (3, 300);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj2_filter_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj2_filter_mv")
    order_qt_filter_after_right_incremental """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj2_filter_mv
        ORDER BY k1, left_v1, right_v2
    """

}
