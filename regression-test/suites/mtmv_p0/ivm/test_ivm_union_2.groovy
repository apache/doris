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

suite("test_ivm_union_2") {

    // =========================================================
    // Part 6: MOW + DUP UNION ALL — mixed determinism, insert-only is fine
    //
    // KNOWN LIMITATION: With mock binlog (no real CDC), incremental refresh reads the full
    // base table as "delta". For DUP tables with non-deterministic row_id (uuid_numeric),
    // each incremental refresh inserts new rows that don't match existing MV rows, causing
    // duplicates. The complete_recovery result below confirms the system self-corrects.
    // When real binlog is used, only actual deltas are read and this duplication does not occur.
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_2_mow_dup_mv;"""
    sql """drop table if exists test_ivm_union_2_mow_dup_t1;"""
    sql """drop table if exists test_ivm_union_2_mow_dup_t2;"""

    sql """
        CREATE TABLE test_ivm_union_2_mow_dup_t1 (
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
        CREATE TABLE test_ivm_union_2_mow_dup_t2 (
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
        INSERT INTO test_ivm_union_2_mow_dup_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_2_mow_dup_t2 VALUES
            (3, 30),
            (4, 40);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_2_mow_dup_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_2_mow_dup_t1
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_2_mow_dup_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_mow_dup_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_mow_dup_mv")
    order_qt_union_mow_dup_after_complete """
        SELECT k1, v1 FROM test_ivm_union_2_mow_dup_mv
    """

    sql """INSERT INTO test_ivm_union_2_mow_dup_t2 VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_mow_dup_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_mow_dup_mv")
    order_qt_union_mow_dup_after_inc """
        SELECT k1, v1 FROM test_ivm_union_2_mow_dup_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_mow_dup_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_mow_dup_mv")
    order_qt_union_mow_dup_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_2_mow_dup_mv
    """

    // =========================================================
    // Part 7: (JOIN) UNION ALL (JOIN) — both arms are joins
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_2_jj_mv;"""
    sql """drop table if exists test_ivm_union_2_jj_t1;"""
    sql """drop table if exists test_ivm_union_2_jj_t2;"""
    sql """drop table if exists test_ivm_union_2_jj_t3;"""
    sql """drop table if exists test_ivm_union_2_jj_t4;"""

    sql """
        CREATE TABLE test_ivm_union_2_jj_t1 (
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
        CREATE TABLE test_ivm_union_2_jj_t2 (
            k1 INT,
            v2 INT
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
        CREATE TABLE test_ivm_union_2_jj_t3 (
            k1 INT,
            v3 INT
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
        CREATE TABLE test_ivm_union_2_jj_t4 (
            k1 INT,
            v4 INT
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

    sql """INSERT INTO test_ivm_union_2_jj_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_union_2_jj_t2 VALUES (1, 100), (2, 200);"""
    sql """INSERT INTO test_ivm_union_2_jj_t3 VALUES (3, 300), (4, 400);"""
    sql """INSERT INTO test_ivm_union_2_jj_t4 VALUES (3, 30), (4, 40);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_2_jj_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT test_ivm_union_2_jj_t1.k1, test_ivm_union_2_jj_t1.v1 + test_ivm_union_2_jj_t2.v2 AS total
        FROM test_ivm_union_2_jj_t1 INNER JOIN test_ivm_union_2_jj_t2
        ON test_ivm_union_2_jj_t1.k1 = test_ivm_union_2_jj_t2.k1
        UNION ALL
        SELECT test_ivm_union_2_jj_t3.k1, test_ivm_union_2_jj_t3.v3 + test_ivm_union_2_jj_t4.v4 AS total
        FROM test_ivm_union_2_jj_t3 INNER JOIN test_ivm_union_2_jj_t4
        ON test_ivm_union_2_jj_t3.k1 = test_ivm_union_2_jj_t4.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_jj_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_jj_mv")
    order_qt_union_jj_after_complete """
        SELECT k1, total FROM test_ivm_union_2_jj_mv
    """

    // Update left join arm and add a new row to right join arm
    sql """INSERT INTO test_ivm_union_2_jj_t1 VALUES (2, 25);"""
    sql """INSERT INTO test_ivm_union_2_jj_t3 VALUES (5, 500);"""
    sql """INSERT INTO test_ivm_union_2_jj_t4 VALUES (5, 50);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_jj_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_jj_mv")
    order_qt_union_jj_after_inc """
        SELECT k1, total FROM test_ivm_union_2_jj_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_jj_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_jj_mv")
    order_qt_union_jj_after_complete_recovery """
        SELECT k1, total FROM test_ivm_union_2_jj_mv
    """

    // =========================================================
    // Part 8: UNION ALL with JOIN in one arm
    // One arm is a join of two tables, the other arm is a single scan
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_2_join_arm_mv;"""
    sql """drop table if exists test_ivm_union_2_join_t1;"""
    sql """drop table if exists test_ivm_union_2_join_t2;"""
    sql """drop table if exists test_ivm_union_2_join_t3;"""

    sql """
        CREATE TABLE test_ivm_union_2_join_t1 (
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
        CREATE TABLE test_ivm_union_2_join_t2 (
            k1 INT,
            v2 INT
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
        CREATE TABLE test_ivm_union_2_join_t3 (
            k1 INT,
            v3 INT
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
        INSERT INTO test_ivm_union_2_join_t1 VALUES (1, 10), (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_2_join_t2 VALUES (1, 100), (2, 200);
    """
    sql """
        INSERT INTO test_ivm_union_2_join_t3 VALUES (3, 300), (4, 400);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_2_join_arm_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT test_ivm_union_2_join_t1.k1, test_ivm_union_2_join_t1.v1 + test_ivm_union_2_join_t2.v2 AS total
        FROM test_ivm_union_2_join_t1 INNER JOIN test_ivm_union_2_join_t2
        ON test_ivm_union_2_join_t1.k1 = test_ivm_union_2_join_t2.k1
        UNION ALL
        SELECT k1, v3 AS total FROM test_ivm_union_2_join_t3;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_join_arm_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_join_arm_mv")
    order_qt_union_join_arm_after_complete """
        SELECT k1, total FROM test_ivm_union_2_join_arm_mv
    """

    // Insert into join arm (t1) and single-scan arm (t3)
    sql """INSERT INTO test_ivm_union_2_join_t1 VALUES (2, 25);"""
    sql """INSERT INTO test_ivm_union_2_join_t3 VALUES (5, 500);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_join_arm_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_join_arm_mv")
    order_qt_union_join_arm_after_inc """
        SELECT k1, total FROM test_ivm_union_2_join_arm_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_join_arm_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_join_arm_mv")
    order_qt_union_join_arm_after_complete_recovery """
        SELECT k1, total FROM test_ivm_union_2_join_arm_mv
    """

    // =========================================================
    // Part 9: UNION ALL with different column names — aliased in each arm
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_2_alias_mv;"""
    sql """drop table if exists test_ivm_union_2_alias_t1;"""
    sql """drop table if exists test_ivm_union_2_alias_t2;"""

    sql """
        CREATE TABLE test_ivm_union_2_alias_t1 (
            k1 INT,
            val INT
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
        CREATE TABLE test_ivm_union_2_alias_t2 (
            k2 INT,
            amount INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_union_2_alias_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_union_2_alias_t2 VALUES
            (3, 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_2_alias_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1 AS id, val AS value FROM test_ivm_union_2_alias_t1
        UNION ALL
        SELECT k2 AS id, amount AS value FROM test_ivm_union_2_alias_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_alias_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_alias_mv")
    order_qt_union_alias_after_complete """
        SELECT id, value FROM test_ivm_union_2_alias_mv
    """

    sql """INSERT INTO test_ivm_union_2_alias_t2 VALUES (4, 40);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_alias_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_alias_mv")
    order_qt_union_alias_after_inc """
        SELECT id, value FROM test_ivm_union_2_alias_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_alias_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_alias_mv")
    order_qt_union_alias_after_complete_recovery """
        SELECT id, value FROM test_ivm_union_2_alias_mv
    """

    // =========================================================
    // Part 10: Delete in UNION ALL arm (MOW with binlog_op)
    // =========================================================
    sql """drop materialized view if exists test_ivm_union_2_delete_mv;"""
    sql """drop table if exists test_ivm_union_2_delete_t1;"""
    sql """drop table if exists test_ivm_union_2_delete_t2;"""

    sql """
        CREATE TABLE test_ivm_union_2_delete_t1 (
            k1 INT,
            v1 INT,
            binlog_op TINYINT
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
        CREATE TABLE test_ivm_union_2_delete_t2 (
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
        INSERT INTO test_ivm_union_2_delete_t1 VALUES
            (1, 10, 0),
            (2, 20, 0);
    """
    sql """
        INSERT INTO test_ivm_union_2_delete_t2 VALUES
            (3, 30),
            (4, 40);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_union_2_delete_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k1, v1 FROM test_ivm_union_2_delete_t1
        UNION ALL
        SELECT k1, v1 FROM test_ivm_union_2_delete_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_delete_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_delete_mv")
    order_qt_union_delete_after_complete """
        SELECT k1, v1 FROM test_ivm_union_2_delete_mv
    """

    sql """
        INSERT INTO test_ivm_union_2_delete_t1 VALUES
            (2, 20, 1),
            (5, 50, 0);
    """
    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_delete_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_delete_mv")
    order_qt_union_delete_after_inc """
        SELECT k1, v1 FROM test_ivm_union_2_delete_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_union_2_delete_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_union_2_delete_mv")
    order_qt_union_delete_after_complete_recovery """
        SELECT k1, v1 FROM test_ivm_union_2_delete_mv
    """
}
