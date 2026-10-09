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

suite("test_ivm_dup_keys_lsn_rowid") {
    // DUP_KEYS base tables use the row-binlog lsn column (__DORIS_ROW_LSN_COL__) as the
    // deterministic IVM row-id. DUP + binlog<row> tables reject predicate DELETE
    // (DeleteHandler), so deletion is exercised from the MOW side of joins (parts 3/4);
    // parts 1/2 cover incremental INSERT and COMPLETE recovery.

    // =========================================================
    // Part 1: simple scan MV — incremental INSERT
    // =========================================================
    sql """drop materialized view if exists test_ivm_dup_lsn_scan_mv;"""
    sql """drop table if exists test_ivm_dup_lsn_scan_base;"""

    sql """
        CREATE TABLE test_ivm_dup_lsn_scan_base (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
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
        INSERT INTO test_ivm_dup_lsn_scan_base VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_lsn_scan_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT * FROM test_ivm_dup_lsn_scan_base;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_scan_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_scan_mv")
    order_qt_scan_after_complete """SELECT k1, v1, v2 FROM test_ivm_dup_lsn_scan_mv"""

    sql """INSERT INTO test_ivm_dup_lsn_scan_base VALUES (4, 40, 'ddd'), (1, 11, 'aaa_dup');"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_scan_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_scan_mv")
    order_qt_scan_after_incremental_insert """SELECT k1, v1, v2 FROM test_ivm_dup_lsn_scan_mv"""

    // Recovery check: COMPLETE refresh must agree with the incremental result.
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_scan_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_scan_mv")
    order_qt_scan_after_complete_recovery """SELECT k1, v1, v2 FROM test_ivm_dup_lsn_scan_mv"""

    // =========================================================
    // Part 2: DUP × DUP inner join MV — incremental INSERT
    // =========================================================
    sql """drop materialized view if exists test_ivm_dup_lsn_join_mv;"""
    sql """drop table if exists test_ivm_dup_lsn_join_l;"""
    sql """drop table if exists test_ivm_dup_lsn_join_r;"""

    sql """
        CREATE TABLE test_ivm_dup_lsn_join_l (
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
        CREATE TABLE test_ivm_dup_lsn_join_r (
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

    sql """INSERT INTO test_ivm_dup_lsn_join_l VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO test_ivm_dup_lsn_join_r VALUES (1, 100), (2, 200), (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_lsn_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_dup_lsn_join_l.k1 AS k1,
            test_ivm_dup_lsn_join_l.v1 AS left_v1,
            test_ivm_dup_lsn_join_r.v2 AS right_v2
        FROM test_ivm_dup_lsn_join_l
        INNER JOIN test_ivm_dup_lsn_join_r
            ON test_ivm_dup_lsn_join_l.k1 = test_ivm_dup_lsn_join_r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_join_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_join_mv")
    order_qt_join_after_complete """SELECT k1, left_v1, right_v2 FROM test_ivm_dup_lsn_join_mv"""

    // Incremental INSERT on the right side.
    sql """INSERT INTO test_ivm_dup_lsn_join_r VALUES (4, 400);"""
    sql """INSERT INTO test_ivm_dup_lsn_join_l VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_join_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_join_mv")
    order_qt_join_after_incremental_insert """SELECT k1, left_v1, right_v2 FROM test_ivm_dup_lsn_join_mv"""

    // Recovery check: COMPLETE refresh must agree with the incremental result.
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_join_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_join_mv")
    order_qt_join_after_complete_recovery """SELECT k1, left_v1, right_v2 FROM test_ivm_dup_lsn_join_mv"""

    // =========================================================
    // Part 3: MOW INNER JOIN DUP — DELETE on the MOW side retracts the joined row
    // =========================================================
    sql """drop materialized view if exists test_ivm_dup_lsn_mow_inner_mv;"""
    sql """drop table if exists test_ivm_dup_lsn_mow_inner_l;"""
    sql """drop table if exists test_ivm_dup_lsn_mow_inner_r;"""

    sql """
        CREATE TABLE test_ivm_dup_lsn_mow_inner_l (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """
    sql """
        CREATE TABLE test_ivm_dup_lsn_mow_inner_r (
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

    sql """INSERT INTO test_ivm_dup_lsn_mow_inner_l VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO test_ivm_dup_lsn_mow_inner_r VALUES (1, 100), (2, 200), (4, 400);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_lsn_mow_inner_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_dup_lsn_mow_inner_l.k1 AS k1,
            test_ivm_dup_lsn_mow_inner_l.v1 AS mow_v,
            test_ivm_dup_lsn_mow_inner_r.v2 AS dup_v
        FROM test_ivm_dup_lsn_mow_inner_l
        INNER JOIN test_ivm_dup_lsn_mow_inner_r
            ON test_ivm_dup_lsn_mow_inner_l.k1 = test_ivm_dup_lsn_mow_inner_r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_inner_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_inner_mv")
    order_qt_mow_inner_after_complete """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_inner_mv"""

    // DELETE on the MOW side must retract the joined row.
    sql """DELETE FROM test_ivm_dup_lsn_mow_inner_l WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_inner_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_inner_mv")
    order_qt_mow_inner_after_delete """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_inner_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_inner_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_inner_mv")
    order_qt_mow_inner_after_complete_recovery """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_inner_mv"""

    // =========================================================
    // Part 4: MOW LEFT OUTER JOIN DUP — DELETE on the MOW side retracts matched rows
    // =========================================================
    sql """drop materialized view if exists test_ivm_dup_lsn_mow_loj_mv;"""
    sql """drop table if exists test_ivm_dup_lsn_mow_loj_l;"""
    sql """drop table if exists test_ivm_dup_lsn_mow_loj_r;"""

    sql """
        CREATE TABLE test_ivm_dup_lsn_mow_loj_l (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """
    sql """
        CREATE TABLE test_ivm_dup_lsn_mow_loj_r (
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

    sql """INSERT INTO test_ivm_dup_lsn_mow_loj_l VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO test_ivm_dup_lsn_mow_loj_r VALUES (1, 100), (2, 200);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_lsn_mow_loj_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_dup_lsn_mow_loj_l.k1 AS k1,
            test_ivm_dup_lsn_mow_loj_l.v1 AS mow_v,
            test_ivm_dup_lsn_mow_loj_r.v2 AS dup_v
        FROM test_ivm_dup_lsn_mow_loj_l
        LEFT OUTER JOIN test_ivm_dup_lsn_mow_loj_r
            ON test_ivm_dup_lsn_mow_loj_l.k1 = test_ivm_dup_lsn_mow_loj_r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_loj_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_loj_mv")
    order_qt_mow_loj_after_complete """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_loj_mv"""

    // DELETE a matched MOW row: the joined row must be retracted, unmatched rows stay.
    sql """DELETE FROM test_ivm_dup_lsn_mow_loj_l WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_loj_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_loj_mv")
    order_qt_mow_loj_after_delete """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_loj_mv"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_lsn_mow_loj_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_lsn_mow_loj_mv")
    order_qt_mow_loj_after_complete_recovery """SELECT k1, mow_v, dup_v FROM test_ivm_dup_lsn_mow_loj_mv"""
}
