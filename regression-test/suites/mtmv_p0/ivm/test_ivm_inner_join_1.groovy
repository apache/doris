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

suite("test_ivm_inner_join_1") {

    // =========================================================
    // Part 1: MOW x MOW inner join — two successive INCREMENTAL refreshes
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_1_basic_mv;"""
    sql """drop table if exists test_ivm_inner_join_1_basic_t1;"""
    sql """drop table if exists test_ivm_inner_join_1_basic_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_1_basic_t1 (
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
        CREATE TABLE test_ivm_inner_join_1_basic_t2 (
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
        INSERT INTO test_ivm_inner_join_1_basic_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_inner_join_1_basic_t2 VALUES
            (1, 100),
            (3, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_1_basic_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_1_basic_t1.k1 AS k1,
            test_ivm_inner_join_1_basic_t1.v1 AS left_v1,
            test_ivm_inner_join_1_basic_t2.v2 AS right_v2
        FROM test_ivm_inner_join_1_basic_t1
        INNER JOIN test_ivm_inner_join_1_basic_t2
            ON test_ivm_inner_join_1_basic_t1.k1 = test_ivm_inner_join_1_basic_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_basic_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_basic_mv")
    order_qt_inner_basic_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_basic_mv
    """

    sql """INSERT INTO test_ivm_inner_join_1_basic_t1 VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_basic_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_basic_mv")
    order_qt_inner_basic_after_first_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_basic_mv
    """

    sql """
        INSERT INTO test_ivm_inner_join_1_basic_t2 VALUES
            (1, 111),
            (2, 220);
    """
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_basic_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_basic_mv")
    order_qt_inner_basic_after_second_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_basic_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_basic_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_basic_mv")
    order_qt_inner_basic_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_basic_mv
    """

    // =========================================================
    // Part 2: MOW x MOW cross join — cartesian product remains correct
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_1_cross_mv;"""
    sql """drop table if exists test_ivm_inner_join_1_cross_t1;"""
    sql """drop table if exists test_ivm_inner_join_1_cross_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_1_cross_t1 (
            k1 INT
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
        CREATE TABLE test_ivm_inner_join_1_cross_t2 (
            k1 INT
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

    sql """INSERT INTO test_ivm_inner_join_1_cross_t1 VALUES (1), (2);"""
    sql """INSERT INTO test_ivm_inner_join_1_cross_t2 VALUES (10), (20);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_1_cross_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_1_cross_t1.k1 AS left_k1,
            test_ivm_inner_join_1_cross_t2.k1 AS right_k1
        FROM test_ivm_inner_join_1_cross_t1
        CROSS JOIN test_ivm_inner_join_1_cross_t2;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_cross_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_cross_mv")
    order_qt_cross_after_complete """
        SELECT left_k1, right_k1 FROM test_ivm_inner_join_1_cross_mv
    """

    sql """INSERT INTO test_ivm_inner_join_1_cross_t1 VALUES (3);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_cross_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_cross_mv")
    order_qt_cross_after_incremental """
        SELECT left_k1, right_k1 FROM test_ivm_inner_join_1_cross_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_cross_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_cross_mv")
    order_qt_cross_after_complete_recovery """
        SELECT left_k1, right_k1 FROM test_ivm_inner_join_1_cross_mv
    """

    // =========================================================
    // Part 3: MOW x DUP — unmatched delete on MOW side should not fallback
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_1_unmatched_mv;"""
    sql """drop table if exists test_ivm_inner_join_1_unmatched_t1;"""
    sql """drop table if exists test_ivm_inner_join_1_unmatched_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_1_unmatched_t1 (
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
        CREATE TABLE test_ivm_inner_join_1_unmatched_t2 (
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
        INSERT INTO test_ivm_inner_join_1_unmatched_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_inner_join_1_unmatched_t2 VALUES
            (1, 100),
            (2, 200),
            (3, 300);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_1_unmatched_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_1_unmatched_t1.k1 AS k1,
            test_ivm_inner_join_1_unmatched_t1.v1 AS left_v1,
            test_ivm_inner_join_1_unmatched_t2.v2 AS right_v2
        FROM test_ivm_inner_join_1_unmatched_t1
        INNER JOIN test_ivm_inner_join_1_unmatched_t2
            ON test_ivm_inner_join_1_unmatched_t1.k1 = test_ivm_inner_join_1_unmatched_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_unmatched_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_unmatched_mv")
    order_qt_mow_dup_unmatched_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_unmatched_mv
    """

    sql """
        INSERT INTO test_ivm_inner_join_1_unmatched_t1 VALUES
            (3, 30);
    """
    sql """DELETE FROM test_ivm_inner_join_1_unmatched_t1 WHERE k1 = 9;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_unmatched_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_unmatched_mv")

    // Verify the INCREMENTAL refresh actually succeeded (not silently fell back to COMPLETE)
    def unmatchedTaskSql = """
        select TaskId, Status, ErrorMsg from tasks('type'='mv')
        where MvDatabaseName = '${context.dbName}' and MvName = 'test_ivm_inner_join_1_unmatched_mv'
        order by CreateTime DESC limit 1
    """
    def unmatchedTaskResult = sql(unmatchedTaskSql)
    assertTrue(unmatchedTaskResult[0][1].toString() == "SUCCESS",
            "Expected unmatched delete INCREMENTAL to succeed, but got: "
                    + unmatchedTaskResult[0][1] + ", error: " + unmatchedTaskResult[0][2])

    order_qt_mow_dup_unmatched_after_incremental """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_unmatched_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_unmatched_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_unmatched_mv")
    order_qt_mow_dup_unmatched_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_unmatched_mv
    """

    // =========================================================
    // Part 4: MOW x DUP — matched delete on MOW side should fail explicit INCREMENTAL
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_1_matched_mv;"""
    sql """drop table if exists test_ivm_inner_join_1_matched_t1;"""
    sql """drop table if exists test_ivm_inner_join_1_matched_t2;"""

    sql """
        CREATE TABLE test_ivm_inner_join_1_matched_t1 (
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
        CREATE TABLE test_ivm_inner_join_1_matched_t2 (
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
        INSERT INTO test_ivm_inner_join_1_matched_t1 VALUES
            (1, 10),
            (2, 20);
    """
    sql """
        INSERT INTO test_ivm_inner_join_1_matched_t2 VALUES
            (1, 100),
            (2, 200);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_1_matched_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_1_matched_t1.k1 AS k1,
            test_ivm_inner_join_1_matched_t1.v1 AS left_v1,
            test_ivm_inner_join_1_matched_t2.v2 AS right_v2
        FROM test_ivm_inner_join_1_matched_t1
        INNER JOIN test_ivm_inner_join_1_matched_t2
            ON test_ivm_inner_join_1_matched_t1.k1 = test_ivm_inner_join_1_matched_t2.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_matched_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_matched_mv")
    order_qt_mow_dup_matched_after_complete """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_matched_mv
    """

    sql """DELETE FROM test_ivm_inner_join_1_matched_t1 WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_matched_mv INCREMENTAL"""

    def matchedTaskSql = """
        select TaskId, Status, ErrorMsg from tasks('type'='mv')
        where MvDatabaseName = '${context.dbName}' and MvName = 'test_ivm_inner_join_1_matched_mv'
        order by CreateTime DESC limit 1
    """
    def matchedTaskResult
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        matchedTaskResult = sql(matchedTaskSql)
        if (matchedTaskResult.isEmpty()) {
            return false
        }
        def st = matchedTaskResult[0][1].toString()
        return st != 'PENDING' && st != 'RUNNING'
    })
    def matchedTaskStatus = matchedTaskResult[0][1].toString()
    def matchedErrorMsg = matchedTaskResult[0][2].toString()
    assertTrue(matchedTaskStatus == "FAILED",
            "Expected explicit INCREMENTAL to fail for matched delete on non-deterministic row_id, but got: "
                    + matchedTaskStatus)
    assertTrue(matchedErrorMsg.contains("IVM fallback: delete on non-deterministic row_id")
                    || matchedErrorMsg.contains("assert_true"),
            "Expected join fallback message in task error, but got: " + matchedErrorMsg)

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_matched_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_matched_mv")
    order_qt_mow_dup_matched_after_complete_recovery """
        SELECT k1, left_v1, right_v2 FROM test_ivm_inner_join_1_matched_mv
    """

    // =========================================================
    // Part 5: 3-table nested inner join — all MOW stays deterministic
    // =========================================================
    sql """drop materialized view if exists test_ivm_inner_join_1_nested_mv;"""
    sql """drop table if exists test_ivm_inner_join_1_nested_t1;"""
    sql """drop table if exists test_ivm_inner_join_1_nested_t2;"""
    sql """drop table if exists test_ivm_inner_join_1_nested_t3;"""

    sql """
        CREATE TABLE test_ivm_inner_join_1_nested_t1 (
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
        CREATE TABLE test_ivm_inner_join_1_nested_t2 (
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
        CREATE TABLE test_ivm_inner_join_1_nested_t3 (
            k1 INT,
            v3 INT
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

    sql """INSERT INTO test_ivm_inner_join_1_nested_t1 VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_inner_join_1_nested_t2 VALUES (1, 100), (2, 200);"""
    sql """INSERT INTO test_ivm_inner_join_1_nested_t3 VALUES (1, 1000);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_inner_join_1_nested_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            test_ivm_inner_join_1_nested_t1.k1 AS k1,
            test_ivm_inner_join_1_nested_t1.v1 AS left_v1,
            test_ivm_inner_join_1_nested_t2.v2 AS mid_v2,
            test_ivm_inner_join_1_nested_t3.v3 AS right_v3
        FROM test_ivm_inner_join_1_nested_t1
        INNER JOIN test_ivm_inner_join_1_nested_t2
            ON test_ivm_inner_join_1_nested_t1.k1 = test_ivm_inner_join_1_nested_t2.k1
        INNER JOIN test_ivm_inner_join_1_nested_t3
            ON test_ivm_inner_join_1_nested_t2.k1 = test_ivm_inner_join_1_nested_t3.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_nested_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_nested_mv")
    order_qt_nested_join_after_complete """
        SELECT k1, left_v1, mid_v2, right_v3 FROM test_ivm_inner_join_1_nested_mv
    """

    sql """INSERT INTO test_ivm_inner_join_1_nested_t3 VALUES (2, 2000);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_nested_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_nested_mv")
    order_qt_nested_join_after_incremental """
        SELECT k1, left_v1, mid_v2, right_v3 FROM test_ivm_inner_join_1_nested_mv
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_inner_join_1_nested_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_inner_join_1_nested_mv")
    order_qt_nested_join_after_complete_recovery """
        SELECT k1, left_v1, mid_v2, right_v3 FROM test_ivm_inner_join_1_nested_mv
    """
}
