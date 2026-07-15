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

suite("test_ivm_outer_join_3") {

    // =========================================================
    // Part 1: RIGHT OUTER JOIN null-side delta with pure hash conjuncts.
    // =========================================================
    sql """drop materialized view if exists ivm_oj3_right_event_mv;"""
    sql """drop table if exists ivm_oj3_right_event_l;"""
    sql """drop table if exists ivm_oj3_right_event_r;"""

    sql """
        CREATE TABLE ivm_oj3_right_event_l (
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
        CREATE TABLE ivm_oj3_right_event_r (
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

    sql """INSERT INTO ivm_oj3_right_event_l VALUES (1, 10), (3, 30);"""
    sql """INSERT INTO ivm_oj3_right_event_r VALUES (1, 100), (2, 200), (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj3_right_event_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_oj3_right_event_r.k1 AS k1,
            ivm_oj3_right_event_l.v1 AS left_v1,
            ivm_oj3_right_event_r.v2 AS right_v2
        FROM ivm_oj3_right_event_l
        RIGHT OUTER JOIN ivm_oj3_right_event_r
            ON ivm_oj3_right_event_l.k1 = ivm_oj3_right_event_r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj3_right_event_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj3_right_event_mv")
    advance_ivm_stream_offset("ivm_oj3_right_event_mv")
    order_qt_right_event_after_complete """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj3_right_event_mv
        ORDER BY k1, left_v1, right_v2
    """

    sql """INSERT INTO ivm_oj3_right_event_r VALUES (4, 400);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj3_right_event_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj3_right_event_mv")
    order_qt_right_event_after_right_incremental """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj3_right_event_mv
        ORDER BY k1, left_v1, right_v2
    """

    sql """INSERT INTO ivm_oj3_right_event_l VALUES (2, 20);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj3_right_event_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj3_right_event_mv")
    order_qt_right_event_after_left_incremental """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj3_right_event_mv
        ORDER BY k1, left_v1, right_v2
    """

    // =========================================================
    // Part 2: RIGHT OUTER JOIN null-side delta with a non-hash other conjunct.
    // =========================================================
    sql """drop materialized view if exists ivm_oj3_right_repair_mv;"""
    sql """drop table if exists ivm_oj3_right_repair_l;"""
    sql """drop table if exists ivm_oj3_right_repair_r;"""

    sql """
        CREATE TABLE ivm_oj3_right_repair_l (
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
        CREATE TABLE ivm_oj3_right_repair_r (
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

    sql """INSERT INTO ivm_oj3_right_repair_l VALUES (1, 10);"""
    sql """INSERT INTO ivm_oj3_right_repair_r VALUES (1, 5), (2, 25), (3, 30);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj3_right_repair_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS
        SELECT
            ivm_oj3_right_repair_r.k1 AS k1,
            ivm_oj3_right_repair_l.v1 AS left_v1,
            ivm_oj3_right_repair_r.v2 AS right_v2
        FROM ivm_oj3_right_repair_l
        RIGHT OUTER JOIN ivm_oj3_right_repair_r
            ON ivm_oj3_right_repair_l.k1 = ivm_oj3_right_repair_r.k1
            AND ivm_oj3_right_repair_l.v1 > ivm_oj3_right_repair_r.v2;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj3_right_repair_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj3_right_repair_mv")
    advance_ivm_stream_offset("ivm_oj3_right_repair_mv")
    order_qt_right_repair_after_complete """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj3_right_repair_mv
        ORDER BY k1, left_v1, right_v2
    """

    sql """INSERT INTO ivm_oj3_right_repair_l VALUES (2, 30);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj3_right_repair_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj3_right_repair_mv")
    order_qt_right_repair_after_left_incremental """
        SELECT k1, left_v1, right_v2
        FROM ivm_oj3_right_repair_mv
        ORDER BY k1, left_v1, right_v2
    """

}
