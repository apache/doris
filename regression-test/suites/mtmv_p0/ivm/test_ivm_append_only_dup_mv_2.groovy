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

suite("test_ivm_append_only_dup_mv_2") {
    // The operators of the append-only detail model that combine two inputs: an inner join, a cross join and
    // UNION ALL. For the joins each side's delta joins the other side's snapshot, which is what makes the
    // pair that matches appear exactly once; for UNION ALL each arm contributes its own delta. The table
    // model itself and the single-table operators are in _1; chained MVs, the model a user can ask for and a
    // broken premise are in _3.

    // =====================================================================================
    // Part 1: inner join of two append-only detail tables
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_join;"""
    sql """drop table if exists dup_base_join_l;"""
    sql """drop table if exists dup_base_join_r;"""
    sql """
        CREATE TABLE dup_base_join_l (
            id BIGINT NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE TABLE dup_base_join_r (
            id BIGINT NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_join
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT l.id AS id, l.v1 AS v1, r.v2 AS v2
        FROM dup_base_join_l l JOIN dup_base_join_r r ON l.id = r.id;
    """
    def dupJoinDesc = sql """desc dup_mv_join all"""
    logger.info("desc dup_mv_join: " + dupJoinDesc.toString())
    assertTrue(dupJoinDesc.toString().contains("DUP_KEYS"))
    assertFalse(dupJoinDesc.toString().contains("IVM"))

    sql """INSERT INTO dup_base_join_l VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO dup_base_join_r VALUES (1, 100), (2, 200), (3, 300);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_after_complete """SELECT id, v1, v2 FROM dup_mv_join"""

    // Only the left table appends: the delta is the left's rows joined against the right's snapshot.
    sql """INSERT INTO dup_base_join_l VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_left_only """SELECT id, v1, v2 FROM dup_mv_join"""

    // Only the right table appends.
    sql """INSERT INTO dup_base_join_r VALUES (4, 400);"""
    sql """INSERT INTO dup_base_join_l VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_right_only """SELECT id, v1, v2 FROM dup_mv_join"""

    // Both sides append inside one window: each side's delta joins the other side's snapshot, and the
    // pair that matches only appears once.
    sql """INSERT INTO dup_base_join_l VALUES (5, 50);"""
    sql """INSERT INTO dup_base_join_r VALUES (5, 500);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_both_sides """SELECT id, v1, v2 FROM dup_mv_join"""

    // A left row that matches nothing contributes nothing, and an id that arrives later still joins.
    sql """INSERT INTO dup_base_join_l VALUES (9, 90);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_unmatched_l """SELECT id, v1, v2 FROM dup_mv_join"""
    sql """INSERT INTO dup_base_join_r VALUES (9, 900);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_join INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_join")
    order_qt_dup_join_late_match """SELECT id, v1, v2 FROM dup_mv_join"""

    // =====================================================================================
    // Part 2: cross join
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_cross;"""
    sql """drop table if exists dup_base_cross_l;"""
    sql """drop table if exists dup_base_cross_r;"""
    sql """
        CREATE TABLE dup_base_cross_l (
            id BIGINT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE TABLE dup_base_cross_r (
            id BIGINT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_cross
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(l_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT l.id AS l_id, r.id AS r_id FROM dup_base_cross_l l CROSS JOIN dup_base_cross_r r;
    """
    sql """INSERT INTO dup_base_cross_l VALUES (1), (2);"""
    sql """INSERT INTO dup_base_cross_r VALUES (10), (20);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_complete """SELECT l_id, r_id FROM dup_mv_cross"""

    sql """INSERT INTO dup_base_cross_l VALUES (3);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_left_append """SELECT l_id, r_id FROM dup_mv_cross"""

    sql """INSERT INTO dup_base_cross_r VALUES (30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_cross INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_cross")
    order_qt_dup_cross_after_right_append """SELECT l_id, r_id FROM dup_mv_cross"""

    // =====================================================================================
    // Part 3: UNION ALL
    // =====================================================================================
    sql """drop materialized view if exists dup_mv_union;"""
    sql """drop table if exists dup_base_union_a;"""
    sql """drop table if exists dup_base_union_b;"""
    sql """
        CREATE TABLE dup_base_union_a (
            id BIGINT NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE TABLE dup_base_union_b (
            id BIGINT NOT NULL,
            v1 INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW dup_mv_union
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
        AS
        SELECT id, v1 FROM dup_base_union_a
        UNION ALL
        SELECT id, v1 FROM dup_base_union_b;
    """
    sql """INSERT INTO dup_base_union_a VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO dup_base_union_b VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union COMPLETE"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    order_qt_dup_union_after_complete """SELECT id, v1 FROM dup_mv_union"""

    sql """INSERT INTO dup_base_union_a VALUES (4, 40);"""
    sql """INSERT INTO dup_base_union_b VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW dup_mv_union INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("dup_mv_union")
    order_qt_dup_union_after_incremental """SELECT id, v1, count(*) AS c FROM dup_mv_union GROUP BY id, v1"""
}
