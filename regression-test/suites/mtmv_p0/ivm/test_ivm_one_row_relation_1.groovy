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

suite("test_ivm_one_row_relation_1") {
    // Pure OneRowRelation has no stream. The first manual incremental refresh must establish a complete snapshot;
    // later incremental refreshes must retain the single row without re-inserting it.
    sql """drop materialized view if exists ivm_one_row_1_pure_mv;"""
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_1_pure_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT 1 AS k1, 10 AS v1;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_pure_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_pure_mv")
    order_qt_pure_after_first_incremental """SELECT k1, v1 FROM ivm_one_row_1_pure_mv"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_pure_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_pure_mv")
    order_qt_pure_after_second_incremental """SELECT k1, v1 FROM ivm_one_row_1_pure_mv"""

    // UNION ALL: the constant arm is materialized once in the initial complete refresh. Subsequent refreshes only
    // consume the table delta and must retain exactly one constant row.
    sql """drop materialized view if exists ivm_one_row_1_union_mv;"""
    sql """drop table if exists ivm_one_row_1_union_t;"""
    sql """
        CREATE TABLE ivm_one_row_1_union_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO ivm_one_row_1_union_t VALUES (2, 20), (3, 30);"""
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_1_union_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT k1, v1 FROM ivm_one_row_1_union_t
        UNION ALL SELECT 1, 10;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_union_mv")
    order_qt_union_after_first_incremental """SELECT k1, v1 FROM ivm_one_row_1_union_mv"""
    sql """INSERT INTO ivm_one_row_1_union_t VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_union_mv")
    order_qt_union_after_insert_incremental """SELECT k1, v1 FROM ivm_one_row_1_union_mv"""
    sql """UPDATE ivm_one_row_1_union_t SET v1 = 22 WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_union_mv")
    order_qt_union_after_update_incremental """SELECT k1, v1 FROM ivm_one_row_1_union_mv"""
    sql """DELETE FROM ivm_one_row_1_union_t WHERE k1 = 3;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_union_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_union_mv")
    order_qt_union_after_delete_incremental """SELECT k1, v1 FROM ivm_one_row_1_union_mv"""

    // INNER JOIN: a OneRowRelation is the static side; table insert/update/delete drive the join delta.
    sql """drop materialized view if exists ivm_one_row_1_inner_join_mv;"""
    sql """drop table if exists ivm_one_row_1_inner_join_t;"""
    sql """
        CREATE TABLE ivm_one_row_1_inner_join_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO ivm_one_row_1_inner_join_t VALUES (2, 20);"""
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_1_inner_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT t.k1, t.v1
        FROM (SELECT 1 AS k1) c
        INNER JOIN ivm_one_row_1_inner_join_t t ON c.k1 = t.k1;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_inner_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_inner_join_mv")
    order_qt_inner_join_after_first_incremental """SELECT k1, v1 FROM ivm_one_row_1_inner_join_mv"""
    sql """INSERT INTO ivm_one_row_1_inner_join_t VALUES (1, 10);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_inner_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_inner_join_mv")
    order_qt_inner_join_after_insert_incremental """SELECT k1, v1 FROM ivm_one_row_1_inner_join_mv"""
    sql """UPDATE ivm_one_row_1_inner_join_t SET v1 = 11 WHERE k1 = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_inner_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_inner_join_mv")
    order_qt_inner_join_after_update_incremental """SELECT k1, v1 FROM ivm_one_row_1_inner_join_mv"""
    sql """DELETE FROM ivm_one_row_1_inner_join_t WHERE k1 = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_inner_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_inner_join_mv")
    order_qt_inner_join_after_delete_incremental """SELECT k1, v1 FROM ivm_one_row_1_inner_join_mv"""

    // LEFT JOIN: the OneRowRelation is the preserved side. Table changes must correctly replace the unmatched row.
    sql """drop materialized view if exists ivm_one_row_1_left_join_mv;"""
    sql """drop table if exists ivm_one_row_1_left_join_t;"""
    sql """
        CREATE TABLE ivm_one_row_1_left_join_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_1_left_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT c.k1, t.v1
        FROM (SELECT 1 AS k1) c
        LEFT OUTER JOIN ivm_one_row_1_left_join_t t ON c.k1 = t.k1;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_left_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_left_join_mv")
    order_qt_left_join_after_first_incremental """SELECT k1, v1 FROM ivm_one_row_1_left_join_mv"""
    sql """INSERT INTO ivm_one_row_1_left_join_t VALUES (1, 10);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_left_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_left_join_mv")
    order_qt_left_join_after_insert_incremental """SELECT k1, v1 FROM ivm_one_row_1_left_join_mv"""
    sql """UPDATE ivm_one_row_1_left_join_t SET v1 = 11 WHERE k1 = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_left_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_left_join_mv")
    order_qt_left_join_after_update_incremental """SELECT k1, v1 FROM ivm_one_row_1_left_join_mv"""
    sql """DELETE FROM ivm_one_row_1_left_join_t WHERE k1 = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_left_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_left_join_mv")
    order_qt_left_join_after_delete_incremental """SELECT k1, v1 FROM ivm_one_row_1_left_join_mv"""

    // Aggregate over UNION ALL: initial aggregate includes the constant contribution; later refreshes adjust only
    // the base-table contribution.
    sql """drop materialized view if exists ivm_one_row_1_agg_mv;"""
    sql """drop table if exists ivm_one_row_1_agg_t;"""
    sql """
        CREATE TABLE ivm_one_row_1_agg_t (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO ivm_one_row_1_agg_t VALUES (2, 20), (3, 30);"""
    sql """
        CREATE MATERIALIZED VIEW ivm_one_row_1_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT COUNT(*) AS cnt, SUM(v1) AS sum_v1
        FROM (SELECT 10 AS v1 UNION ALL SELECT v1 FROM ivm_one_row_1_agg_t) u;
    """
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_agg_mv")
    order_qt_agg_after_first_incremental """SELECT cnt, sum_v1 FROM ivm_one_row_1_agg_mv"""
    sql """INSERT INTO ivm_one_row_1_agg_t VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_agg_mv")
    order_qt_agg_after_insert_incremental """SELECT cnt, sum_v1 FROM ivm_one_row_1_agg_mv"""
    sql """UPDATE ivm_one_row_1_agg_t SET v1 = 22 WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_agg_mv")
    order_qt_agg_after_update_incremental """SELECT cnt, sum_v1 FROM ivm_one_row_1_agg_mv"""
    sql """DELETE FROM ivm_one_row_1_agg_t WHERE k1 = 3;"""
    sql """REFRESH MATERIALIZED VIEW ivm_one_row_1_agg_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_one_row_1_agg_mv")
    order_qt_agg_after_delete_incremental """SELECT cnt, sum_v1 FROM ivm_one_row_1_agg_mv"""
}
